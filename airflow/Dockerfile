# 기존 스트리밍 파이프라인 스크립트(anomaly/train_model.py, graph/backfill.py 등)를
# Airflow 태스크에서 그대로 실행하기 위해, 프로젝트 배치 의존성을 얹은 이미지.
FROM apache/airflow:2.10.5

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
