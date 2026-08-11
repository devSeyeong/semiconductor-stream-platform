# -*- coding: utf-8 -*-
"""파이프라인 접속 설정 (환경변수 주입).

Kafka / PostgreSQL / Neo4j 접속 정보를 한 곳에 모은다. 기본값이 전부
`localhost` 라 로컬 개발(.venv + `docker compose up -d`)은 지금까지와 똑같이
동작하고, 컨테이너·EC2 에서는 compose 서비스명을 환경변수로 주입해
**같은 코드**를 그대로 돌린다.

    # 로컬 (기본값)
    .venv/bin/python consumer/consumer.py

    # 컨테이너 안
    KAFKA_BOOTSTRAP=kafka:29092 PG_HOST=postgres NEO4J_URI=bolt://neo4j:7687 \
        python consumer/consumer.py

설정 목록은 .env.example 참고.
"""

import os
from urllib.parse import quote_plus

# --------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------- #

# 컨테이너 내부에서는 kafka:29092 (INTERNAL 리스너), 호스트에서는 localhost:9092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "semiconductor-events")

# --------------------------------------------------------------------- #
# PostgreSQL
# --------------------------------------------------------------------- #

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "semiconductor")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")

# --------------------------------------------------------------------- #
# Neo4j
# --------------------------------------------------------------------- #

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "semiconductor")


# --------------------------------------------------------------------- #
# 접속 문자열 헬퍼 — 드라이버마다 형식이 달라서 한 곳에서 만든다
# --------------------------------------------------------------------- #

def pg_dsn() -> dict:
    """psycopg2.connect(**pg_dsn()) 용 인자."""
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "database": PG_DB,
        "user": PG_USER,
        "password": PG_PASSWORD,
    }


def sqlalchemy_url() -> str:
    """SQLAlchemy / pandas.read_sql 용 URL."""
    return (
        f"postgresql+psycopg2://{quote_plus(PG_USER)}:{quote_plus(PG_PASSWORD)}"
        f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )


def jdbc_url() -> str:
    """Spark JDBC 용 URL.

    stringtype=unspecified: contributions 를 문자열로 보내도 JSONB 컬럼에
    그대로 캐스팅되게 한다.
    """
    return (
        f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        "?stringtype=unspecified"
    )
