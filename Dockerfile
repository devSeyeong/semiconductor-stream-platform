# 코어 파이프라인(simulator / consumer / dashboard) 공용 이미지.
#
# 세 프로세스는 같은 코드베이스를 공유하고 실행 커맨드만 다르므로,
# 이미지를 하나만 만들고 compose 에서 command 로 갈라 쓴다.
FROM python:3.12-slim

WORKDIR /app

# 의존성을 먼저 설치해 레이어 캐시를 살린다 (코드만 바뀌면 재설치 안 함).
# numpy / psycopg2-binary 모두 휠로 설치되므로 빌드 도구가 필요 없다.
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY . .

# 로그가 버퍼에 갇히지 않게 (docker compose logs 로 바로 보이도록)
ENV PYTHONUNBUFFERED=1

# 기본값일 뿐, 실제 실행 커맨드는 compose 의 command 가 정한다.
CMD ["python", "consumer/consumer.py"]
