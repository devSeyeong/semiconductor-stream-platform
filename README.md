#### Semiconductor Stream Platform

반도체 공정 데이터를 시뮬레이션하고,
Kafka 기반 스트리밍 파이프라인 및 Spark Structured Streaming 분석 환경을 구축하는 프로젝트입니다.

---

### Architecture

```text
Simulator
   ↓
Kafka
   ↓
Consumer  (PCA + Hotelling's T² 실시간 이상탐지)
   ├─────────────────────────────┐
   ↓                             ↓
PostgreSQL                    Neo4j (그래프: 웨이퍼 계보·장비·이상)
   ↓                             ↓
Streamlit Dashboard      온톨로지(OWL) → 고장모드/근본원인 추론
                               ↓
                         Claude LLM (GraphRAG: NL질의 + RCA 내러티브)
```

---

### Simulator


* Wafer State Machine
* 공정 Step 기반 데이터 생성
* Equipment Degradation
* SECOM 스타일 결측치 및 노이즈
* Defect Probability 계산


---

### Process Flow

```text
DEPOSITION
   ↓
ETCH
   ↓
LITHOGRAPHY
   ↓
INSPECTION
```

---

### Example Event

```json
{
  "wafer_id": "W00010",
  "step": "ETCH",
  "equipment_id": "ETCH-EQ-3",
  "temperature": 81.2,
  "pressure": 1.02,
  "particle_count": 5,
  "defect_probability": 0.013,
  "label": "PASS"
}
```

---

### Python 가상환경(venv)
생성
```bash
python3 -m venv .venv
```
활성화
```bash
source .venv/bin/activate
```

---

### Kafka Python Client 설치

```bash
python3 -m pip install kafka-python
```

---

### Kafka Topic 생성
접속
```bash
docker exec -it <kafka-container> bash
```

Topic생성
```bash
kafka-topics \
--create \
--topic semiconductor-events \
--bootstrap-server localhost:9092
```
Topic확인
```bash
kafka-topics \
--list \
--bootstrap-server localhost:9092
```

---

### PostgreSQL 연결

Kafka Consumer에서 수신한 이벤트를 PostgreSQL에 저장하도록 구성.

Python PostgreSQL Client 설치:

```bash
pip install psycopg2-binary
```
DB 진입
```bash
docker exec -it semiconductor-stream-platform-postgres-1 psql -U admin -d semiconductor
```

DB 연결
```python
    conn = psycopg2.connect(
        host="localhost",
        database="semiconductor",
        user="admin",
        password="admin"
    )
```
Kafka Consumer에서 메시지를 수신한 뒤  
INSERT 쿼리를 통해 semiconductor_events 테이블에 저장.
---

### 이상탐지 (PCA + Hotelling's T²)

반도체 FDC(Fault Detection & Classification)에서 실제로 쓰는 다변량
통계 기법을 적용. 여러 센서를 동시에 보고 평소 패턴에서 벗어난 정도를
하나의 T² 점수로 계산하며, 관리상한(UCL)을 넘으면 이상으로 판정한다.

```text
anomaly/pca_t2.py       # PCA + T² 모델 (numpy 직접 구현)
anomaly/train_model.py  # 스텝별 정상 데이터로 학습 → anomaly/models/*.pkl
anomaly/models/         # 학습된 스텝별 모델
```

스텝마다 센서 구성이 다르므로 **스텝별로 모델을 따로 학습**한다.
3% 결측치는 학습 평균으로 대체하고, 센서별 **기여도**로 어떤 센서가
이상을 일으켰는지 분해해 보여준다.

#### 1) 의존성 설치

```bash
pip install -r requirements.txt
```

#### 2) 모델 학습 (최초 1회)

```bash
.venv/bin/python anomaly/train_model.py
```

#### 3) 파이프라인 실행

```bash
docker compose up -d                              # Kafka + PostgreSQL
.venv/bin/python simulator/simulator.py           # 이벤트 생성
.venv/bin/python consumer/consumer.py             # 실시간 T² 계산 + 적재
.venv/bin/streamlit run dashboard/app.py          # 대시보드 (localhost:8501)
```

대시보드는 3초마다 자동 갱신되며 T² 관리도, 이상 이벤트 목록,
센서 기여도를 실시간으로 보여준다.

---

### Spark Structured Streaming Consumer

`consumer/consumer.py`(단건 kafka-python 루프)를 Spark Structured Streaming
으로 옮긴 버전. Kafka 수신 → step별 PCA + T² 스코어링 → PostgreSQL 적재를
마이크로배치로 처리한다. 이상탐지는 `anomaly/pca_t2.py` 의 `score_one` 을
그대로 재사용하므로 **단건 consumer 와 결과가 100% 동일**하고, 대시보드는
변경 없이 같은 테이블을 읽는다.

```text
Kafka(semiconductor-events)
   ↓ readStream
from_json → groupBy(step).applyInPandas(PCA+T² 스코어링)   # Spark 워커에서 분산
   ↓ foreachBatch
PostgreSQL(semiconductor_events)  ← JDBC append
```

* 모델은 드라이버에서 로드 후 `broadcast`, step별 `applyInPandas` 로 스코어링
* `contributions`(JSONB)는 JDBC URL 의 `stringtype=unspecified` 로 캐스팅
* Kafka 커넥터/PostgreSQL 드라이버 jar 는 최초 실행 시 자동 다운로드
  (Spark 4.x = **Scala 2.13** 빌드 → `spark-sql-kafka-0-10_2.13`)

```bash
docker compose up -d                              # Kafka + PostgreSQL
.venv/bin/python anomaly/train_model.py           # 모델 최초 1회 학습
.venv/bin/python simulator/simulator.py           # 이벤트 생성
.venv/bin/python spark/spark_consumer.py          # Spark consumer (단건 consumer 대체)
.venv/bin/streamlit run dashboard/app.py          # 대시보드 (localhost:8501)
```

> 단건 consumer(`consumer/consumer.py`)와 Spark consumer 중 **하나만** 실행한다.

---

### 그래프 DB + 온톨로지 + LLM (GraphRAG)

반도체 FDC 의 핵심 질문(어느 장비를 거친 웨이퍼가 fail 났나, 같은 이상이
어디서 또 났나)은 "관계 추적"이라 관계형 조인보다 그래프가 훨씬 잘 맞는다.
consumer 가 PostgreSQL 에 적재하는 이벤트를 **Neo4j 에도 dual-write** 하고,
그 위에 OWL 온톨로지와 Claude LLM 을 3단으로 얹었다.

```text
graph/neo4j_writer.py   # 이벤트 → 그래프 dual-write (장애 격리·멱등 MERGE)
graph/backfill.py       # PostgreSQL 과거 데이터 → 그래프 이관
graph/queries.cypher    # genealogy / commonality / fault propagation 쿼리
graph/fdc_ontology.ttl  # 센서→고장모드→근본원인→권장조치 온톨로지(OWL)
graph/ontology.py       # 온톨로지 로더 + owlrl 추론
graph/llm_rca.py        # Claude(Opus 4.8) GraphRAG: NL질의 + RCA 내러티브
```

**3단 구조**: `Neo4j(사실) → 온톨로지(의미) → Claude(자연어)`. 앞 두 단이
LLM 의 환각 가드레일 — LLM 은 읽기전용 `run_cypher` 도구로 검증된 그래프
사실만 가져와 답하고, 온톨로지 어휘가 시스템 프롬프트에 고정되어 있다.

```bash
docker compose up -d neo4j                        # Neo4j (localhost:7474 브라우저, 7687 bolt)
.venv/bin/python graph/ontology.py                # 온톨로지 추론 데모 (서버 불필요)
.venv/bin/python graph/backfill.py                # 기존 이벤트를 그래프로 이관
export ANTHROPIC_API_KEY=sk-...                   # LLM 레이어용 (또는 `ant auth login`)
.venv/bin/python graph/llm_rca.py ask "L003 랏에서 fail이 제일 많은 장비는?"
.venv/bin/python graph/llm_rca.py explain W00007  # 웨이퍼 근본원인(RCA) 내러티브
```

* consumer 에 dual-write 가 **선택적으로** 붙어있다. Neo4j 가 없거나 죽어도
  `GRAPH_ENABLED=0` 이거나 연결 실패 시 기존 파이프라인은 그대로 동작한다.
* Neo4j 접속: 계정 `neo4j` / 비밀번호 `semiconductor` (docker-compose 기본값).

---

### Airflow (배치 오케스트레이션)

실시간 스트리밍(Kafka → consumer → PostgreSQL)이 본체이고, Airflow 는 그 옆에서
**주기적 배치/MLOps 작업**을 스케줄링한다. DAG `fdc_daily_maintenance` 는 매일
새벽 2시에 다음을 실행한다.

```text
data_quality_check                # 적재 데이터 신선도·센서 범위 검증 (품질 게이트)
      ↓
   ┌──┴───────────────┐
retrain_models     sync_graph      # PCA+T² 재학습 / PostgreSQL→Neo4j 이관 (병렬)
   └──┬───────────────┘
      ↓
anomaly_report                    # 장비·스텝별 일일 이상률 요약 → anomaly_daily_report 테이블
```

* `retrain_models`/`sync_graph` 는 기존 스크립트(`anomaly/train_model.py`,
  `graph/backfill.py`)를 **그대로** 실행 → 스트리밍 파이프라인과 로직 100% 동일.
* 접속 정보는 전부 환경변수(`PG_HOST`, `NEO4J_URI` ...)로 주입되어, 컨테이너의
  compose 서비스명(`postgres`/`neo4j`)으로 연결된다.

```text
airflow/Dockerfile                    # apache/airflow + 배치 의존성(numpy/psycopg2/neo4j)
airflow/requirements-airflow.txt
airflow/dags/fdc_daily_maintenance.py # 4-task DAG
```

Airflow 는 무거우므로 docker-compose 의 **`airflow` profile** 로 분리 — 기본
`docker compose up -d` 에는 뜨지 않고, 아래로만 기동한다.

```bash
docker compose --profile airflow up -d --build      # Airflow + 메타DB (+ postgres/neo4j)
# UI: http://localhost:8080  (admin 비밀번호는 아래로 확인)
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated

# 수동 실행 (예)
docker compose exec airflow airflow dags trigger fdc_daily_maintenance
```

---

### 설정 (환경변수)

Kafka / PostgreSQL / Neo4j 접속 정보는 `config.py` 한 곳에 모여있고 전부
환경변수로 덮어쓸 수 있다. 기본값이 `localhost` 라 **로컬 개발은 아무 설정 없이**
지금까지와 똑같이 동작하고, 컨테이너·EC2 배포 시에는 compose 서비스명을
주입해 **같은 코드**를 그대로 돌린다.

```bash
# 로컬 (venv + docker compose) — 기본값 그대로
.venv/bin/python consumer/consumer.py

# 컨테이너 안에서 실행
KAFKA_BOOTSTRAP=kafka:29092 PG_HOST=postgres NEO4J_URI=bolt://neo4j:7687 \
    python consumer/consumer.py
```

설정 목록은 `.env.example` 참고 (`KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `PG_HOST`,
`PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `NEO4J_URI`, `NEO4J_USER`,
`NEO4J_PASSWORD`, `GRAPH_ENABLED`, `ANTHROPIC_API_KEY`).

**Kafka 리스너 2개**: 브로커는 접속 경로별로 "이 주소로 접속하라"고 되돌려주기
때문에, 호스트와 컨테이너가 같은 브로커를 쓰려면 리스너를 나눠야 한다.

```text
INTERNAL  kafka:29092       # 컨테이너 → 컨테이너 (배포 시 앱 컨테이너가 쓰는 경로)
EXTERNAL  localhost:9092    # 호스트(venv) → 컨테이너 (로컬 개발)
```

EC2 등 원격 호스트에서 외부 클라이언트를 붙일 땐 `KAFKA_EXTERNAL_HOST` 로
브로커가 광고할 주소를 바꾼다 (`KAFKA_EXTERNAL_HOST=10.0.1.23 docker compose up -d`).

---

### Tech Stack

* Python
* Apache Airflow (배치 오케스트레이션 — 재학습·그래프동기화·품질검사·리포트 DAG)
* Apache Kafka
* PostgreSQL
* NumPy (PCA + Hotelling's T²)
* Apache Spark (Structured Streaming Consumer)
* Streamlit + Plotly (Dashboard)
* Neo4j (그래프 DB — 웨이퍼 계보·commonality 분석)
* RDFLib + OWL/owlrl (FDC 온톨로지 추론)
* Anthropic Claude (Opus 4.8, GraphRAG NL질의 + RCA)
* Docker
* AWS EC2

---

### TODO

* [x] Kafka Producer
* [x] PostgreSQL Sink
* [x] 이상탐지 (PCA + Hotelling's T²)
* [x] Dashboard
* [x] Spark Streaming
* [x] 그래프 DB (Neo4j dual-write + genealogy/commonality)
* [x] 온톨로지 (OWL 센서→고장모드→근본원인 추론)
* [x] LLM 레이어 (Claude GraphRAG: NL질의 + RCA)
* [x] Airflow (일일 유지보수 DAG: 품질검사·재학습·그래프동기화·이상률 리포트)
* [ ] EC2 Deployment
* [ ] EKS Deployment
