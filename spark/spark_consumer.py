# -*- coding: utf-8 -*-
"""Spark Structured Streaming consumer.

기존 consumer/consumer.py 를 Spark Structured Streaming 으로 옮긴 버전.

  Kafka(semiconductor-events)
     ↓ readStream
  JSON 파싱 → step 별 PCA + Hotelling's T2 스코어링 (applyInPandas, 분산 처리)
     ↓ foreachBatch
  PostgreSQL(semiconductor_events)  ← JDBC append

이상탐지 로직은 anomaly/pca_t2.py 의 PCAT2Model.score_one 을 그대로 재사용하므로
단건 consumer 와 결과가 100% 동일하다. 대시보드(dashboard/app.py)는 변경 없이
같은 테이블을 읽는다.

실행:
    docker compose up -d                          # Kafka + PostgreSQL
    .venv/bin/python anomaly/train_model.py       # 모델 최초 1회 학습
    .venv/bin/python simulator/simulator.py       # 이벤트 생성
    .venv/bin/python spark/spark_consumer.py      # 본 스크립트
"""

import os
import sys

# Spark 워커/드라이버가 반드시 이 venv 의 파이썬을 쓰도록 강제
# (pyarrow, anomaly 모듈 등이 이 환경에만 설치돼 있음)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import json

import pandas as pd
import psycopg2
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# 프로젝트 루트를 import 경로에 추가
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import config  # noqa: E402
from anomaly.pca_t2 import PCAT2Model  # noqa: E402
from consumer.consumer import ensure_schema  # 테이블 DDL 재사용  # noqa: E402

# --------------------------------------------------------------------- #
# 설정
# --------------------------------------------------------------------- #

KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP
TOPIC = config.KAFKA_TOPIC
MODEL_DIR = os.path.join(ROOT, "anomaly", "models")
STEPS = ["DEPOSITION", "ETCH", "LITHOGRAPHY", "INSPECTION"]
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", os.path.join(ROOT, "spark", "checkpoint"))

# stringtype=unspecified: 문자열 contributions 를 JSONB 컬럼에 그대로 캐스팅
JDBC_URL = config.jdbc_url()
JDBC_PROPS = {
    "user": config.PG_USER,
    "password": config.PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
DB_TABLE = "semiconductor_events"

# Spark 4.x = Scala 2.13 빌드. Kafka 커넥터/JDBC 드라이버를 자동 다운로드.
SPARK_PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}",
    "org.postgresql:postgresql:42.7.4",
])

# Kafka value(JSON) 스키마 — simulator 가 만드는 이벤트
EVENT_SCHEMA = StructType([
    StructField("timestamp", LongType()),
    StructField("wafer_id", StringType()),
    StructField("lot_id", StringType()),
    StructField("step", StringType()),
    StructField("equipment_id", StringType()),
    StructField("equipment_health", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("gas_flow", DoubleType()),
    StructField("rf_power", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("particle_count", IntegerType()),
    StructField("defect_probability", DoubleType()),
    StructField("label", StringType()),
])

# 스코어링 후 DB 적재용 스키마 (= 원본 + 이상탐지 결과 5개 컬럼)
SCORED_SCHEMA = StructType(
    EVENT_SCHEMA.fields + [
        StructField("pca_t2", DoubleType()),
        StructField("t2_ucl", DoubleType()),
        StructField("is_anomaly", BooleanType()),
        StructField("top_contributor", StringType()),
        StructField("contributions", StringType()),  # JSON 문자열 → JSONB
    ]
)

INPUT_COLS = [f.name for f in EVENT_SCHEMA.fields]


# --------------------------------------------------------------------- #
# 모델 로드
# --------------------------------------------------------------------- #

def load_models():
    models = {}
    for step in STEPS:
        path = os.path.join(MODEL_DIR, f"{step}.pkl")
        if os.path.exists(path):
            models[step] = PCAT2Model.load(path)
        else:
            print(f"[WARN] 모델 없음: {path} (먼저 train_model.py 실행)")
    return models


# --------------------------------------------------------------------- #
# step 별 스코어링 (applyInPandas) — Spark 워커에서 분산 실행
# --------------------------------------------------------------------- #

def make_score_group(models_bc):
    """브로드캐스트된 모델 dict 로 step 그룹 스코어링 함수를 만든다."""

    def score_group(pdf: pd.DataFrame) -> pd.DataFrame:
        step = pdf["step"].iloc[0]
        model = models_bc.value.get(step)

        records = []
        for _, row in pdf.iterrows():
            event = {c: row[c] for c in INPUT_COLS}
            if model is not None:
                r = model.score_one(event)
            else:
                r = {
                    "t2": None, "ucl": None, "is_anomaly": None,
                    "top_contributor": None, "contributions": {},
                }
            rec = dict(event)
            rec["pca_t2"] = r["t2"]
            rec["t2_ucl"] = r["ucl"]
            rec["is_anomaly"] = r["is_anomaly"]
            rec["top_contributor"] = r["top_contributor"]
            rec["contributions"] = json.dumps(r["contributions"])
            records.append(rec)

        out = pd.DataFrame(records, columns=[f.name for f in SCORED_SCHEMA.fields])
        # Arrow 변환 안정화: 정수/불리언 컬럼은 nullable dtype 으로
        out["particle_count"] = out["particle_count"].astype("Int32")
        out["is_anomaly"] = out["is_anomaly"].astype("boolean")
        return out

    return score_group


# --------------------------------------------------------------------- #
# 마이크로배치 싱크: PostgreSQL JDBC append
# --------------------------------------------------------------------- #

def make_foreach_batch(score_group):
    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        scored = batch_df.groupBy("step").applyInPandas(score_group, SCORED_SCHEMA)
        (
            scored.write
            .jdbc(url=JDBC_URL, table=DB_TABLE, mode="append", properties=JDBC_PROPS)
        )
        n = scored.count()
        print(f"[batch {batch_id}] {n} rows → {DB_TABLE}")

    return process_batch


# --------------------------------------------------------------------- #
# 메인
# --------------------------------------------------------------------- #

def main():
    models = load_models()

    # 테이블이 없으면 생성 (단건 consumer 와 동일한 DDL 재사용)
    conn = psycopg2.connect(**config.pg_dsn())
    ensure_schema(conn)
    conn.close()

    spark = (
        SparkSession.builder
        .appName("semiconductor-spark-consumer")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    models_bc = spark.sparkContext.broadcast(models)
    score_group = make_score_group(models_bc)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )

    query = (
        events.writeStream
        .foreachBatch(make_foreach_batch(score_group))
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    print("Spark Structured Streaming consumer 시작: 실시간 PCA + T2 이상탐지 중...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
