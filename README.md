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
Spark Structured Streaming
   ↓
PostgreSQL / Dashboard
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

### Consumer 확인

---

### Tech Stack

* Python
* Apache Kafka
* Apache Spark
* PostgreSQL
* Docker
* AWS EC2

---

### TODO

* [ ] Kafka Producer
* [ ] Spark Streaming
* [ ] PostgreSQL Sink
* [ ] Dashboard
* [ ] Airflow
* [ ] EC2 Deployment
