# README2 — 그래프 DB + 온톨로지 + LLM 개발 정리 (임시)

기존 스트리밍 파이프라인(simulator → Kafka → consumer → PostgreSQL → Streamlit)
위에 **그래프 DB · 온톨로지 · LLM** 3단 레이어를 얹은 작업 기록.

> 원칙: 기존 파이프라인은 건드리지 않는다. dual-write / 추가 레이어로만 얹고,
> 새 레이어가 죽어도 기존 동작은 그대로 유지되도록 **장애 격리**.

---

## 왜 붙였나

반도체 FDC(Fault Detection & Classification)의 핵심 질문은 대부분 "관계 추적"이다.

- 이 웨이퍼가 거쳐온 장비는? (**genealogy / 계보**)
- FAIL 난 웨이퍼들이 공통으로 지나간 장비는? (**commonality analysis**)
- 이 장비를 거친 뒤 다운스트림에서 fail 난 건? (**fault propagation**)

관계형 테이블(현재 `semiconductor_events`)로는 self-join 지옥이지만, 그래프에선
한 줄 쿼리다. 그 위에 온톨로지로 "센서 이상 → 고장모드 → 근본원인"의 의미를
부여하고, LLM으로 자연어 인터페이스를 씌운다.

---

## 아키텍처 — 3단 구조

```text
Consumer (PCA + T² 이상탐지)
   ├──────────────────────────────┐
   ↓                              ↓
PostgreSQL                     Neo4j  ← Stage 1 (사실)
   ↓                              ↓
Streamlit                  온톨로지(OWL)  ← Stage 2 (의미)
                                  ↓
                          Claude LLM  ← Stage 3 (자연어, GraphRAG)
```

**핵심: 앞 두 단이 LLM의 환각 가드레일.** LLM은 읽기전용 `run_cypher` 도구로
검증된 그래프 사실만 가져와 답하고, 온톨로지 어휘가 시스템 프롬프트에 고정되어
존재하지 않는 관계/센서/컬럼을 못 지어낸다.

---

## Stage 1 — Neo4j 프로퍼티 그래프

### 그래프 모델

```text
(:Lot {id})
(:Wafer {id})            -[:BELONGS_TO]->      (:Lot)
(:Equipment {id,step,health}) -[:PERFORMS]->   (:Step {name})
(:Step)                  -[:NEXT]->            (:Step)   # 공정 흐름
(:Sensor {name})
(:Run {run_key,timestamp,label,t2,ucl,is_anomaly,defect_probability,top_contributor})
   (:Wafer)-[:HAS_RUN]->(:Run)
   (:Run)-[:AT_STEP]->(:Step)
   (:Run)-[:ON_EQUIPMENT]->(:Equipment)
   (:Run)-[:CAUSED_BY]->(:Sensor)   # 이상일 때만
```

시뮬레이터가 이미 뽑는 필드(`wafer_id`, `lot_id`, `equipment_id`, `step`,
`top_contributor`)가 그대로 노드/엣지 재료가 된다.

### 파일

- `graph/neo4j_writer.py` — `GraphWriter`. 이벤트 1건을 MERGE로 그래프에 반영.
  - **장애 격리**: 모든 쓰기를 try/except로 감싸고, 연결 실패해도 예외를 안 던짐.
  - **선택적**: `GRAPH_ENABLED=0`이면 no-op.
  - **멱등**: `run_key`(wafer+timestamp+step+equipment)로 MERGE → 중복 방지, backfill 안전.
- `graph/backfill.py` — 기존 PostgreSQL `semiconductor_events` 행을 그래프로 이관.
- `graph/queries.cypher` — genealogy / commonality / 장비별 이상률 / fault propagation 쿼리 7종.

### consumer 연동 (비침습)

`consumer/consumer.py`에 3줄만 추가:
- import `GraphWriter`
- main()에서 `graph = GraphWriter()` 생성
- INSERT 커밋 뒤 `graph.write_event(event, result)`

### docker-compose

`neo4j:5.23` 서비스 추가 (7474 브라우저 / 7687 bolt, 계정 `neo4j`/`semiconductor`).

---

## Stage 2 — OWL 온톨로지

이상탐지(PCA+T²)는 "temperature 센서가 T²를 78% 끌어올렸다"까지만 안다.
온톨로지는 그걸 "ThermalDrift(온도 드리프트), 근본원인은 히터 이상,
권장조치는 히터 존별 캘리브레이션"이라는 **도메인 의미**로 번역한다.

### 파일

- `graph/fdc_ontology.ttl` — Turtle/OWL. 클래스·속성·개체:
  - 클래스: `Sensor`, `ProcessStep`, `FailureMode`, `RootCause`
    (+ 하위분류 `ProcessExcursion` / `EquipmentFault` / `ContaminationEvent`)
  - 관계: `indicatesFailureMode`, `hasRootCause`, `measuredAt`, `recommendedAction`
  - 센서 12종 → 고장모드 9종 → 근본원인 8종 매핑
- `graph/ontology.py` — `FDCOntology`. rdflib로 로드, **owlrl로 RDFS/OWL-RL 추론**
  (상위클래스 멤버십 자동 도출). `classify_sensor(name)` / `vocabulary()` 제공.

### 예시 (`python graph/ontology.py`)

```
[temperature] → ThermalDrift (온도 드리프트)
    분류: ProcessExcursion
    근본원인: 히터/온도제어 이상
    권장조치: 히터 존별 온도 캘리브레이션, 열전대 점검
[pressure] → ChamberSealFault (챔버 실링 결함) / 근본원인: 챔버 실링 열화
[vibration] → MechanicalVibrationFault / 근본원인: 베어링/구동부 마모
[particle_count] → ParticleContamination / 근본원인: 파티클 오염원
...
```

---

## Stage 3 — LLM 레이어 (GraphRAG)

- 모델: **Claude Opus 4.8** (`claude-opus-4-8`), adaptive thinking
- Anthropic Python SDK **tool_runner** — request→도구실행→루프를 SDK가 자동 처리
- 도구 2종:
  - `run_cypher(query)` — **읽기전용** Cypher 실행. `CREATE/MERGE/DELETE/SET/...`
    같은 쓰기 절이 있으면 거부 + Neo4j read 트랜잭션으로 이중 차단.
  - `classify_sensor(name)` — 온톨로지 분류를 도구로 노출.
- 시스템 프롬프트에 온톨로지 어휘(센서/스텝/고장모드) 주입 = 가드레일.

### 파일

- `graph/llm_rca.py`
  - `ask(question)` — 자연어 질문 → 그래프 조회 → 자연어 답변
  - `explain_anomaly(wafer_id)` — 웨이퍼 계보+온톨로지 분류를 근거로 RCA 내러티브

```bash
.venv/bin/python graph/llm_rca.py ask "L003 랏에서 fail이 제일 많은 장비는?"
.venv/bin/python graph/llm_rca.py explain W00000
```

---

## 검증 결과

| Stage | 상태 | 방법 |
|---|---|---|
| 1 Neo4j | ✅ end-to-end | 합성 이벤트 80건 → Lot5/Wafer10/Equipment20/Run80, commonality·이상률·계보 쿼리 정상 |
| 2 온톨로지 | ✅ | `ontology.py` 실행, 센서 12종 추론 + owlrl 상위분류 도출 |
| 3 LLM | ⚠️ 배선만 | 임포트·도구·프롬프트·읽기전용 가드 확인. **실호출은 API 키 필요** |

Stage 3 실호출은 `ANTHROPIC_API_KEY`(또는 `ant auth login`)가 있어야 한다.
현재 개발 환경엔 키가 없어 LLM 응답까진 미검증.

---

## 실행 순서

```bash
pip install -r requirements.txt                   # neo4j, rdflib, owlrl, anthropic 추가됨

docker compose up -d neo4j                         # 그래프 DB
.venv/bin/python graph/ontology.py                 # 온톨로지 데모 (서버 불필요)

# 기존 파이프라인과 함께 (dual-write 자동)
docker compose up -d                               # kafka + postgres + neo4j
.venv/bin/python anomaly/train_model.py            # 모델 학습 (최초 1회)
.venv/bin/python simulator/simulator.py            # 이벤트 생성
.venv/bin/python consumer/consumer.py              # PostgreSQL + Neo4j 동시 적재

# 또는 기존 데이터를 그래프로 이관
.venv/bin/python graph/backfill.py

# LLM 레이어
export ANTHROPIC_API_KEY=sk-...
.venv/bin/python graph/llm_rca.py ask "..."
.venv/bin/python graph/llm_rca.py explain W00007
```

---

## 의존성 추가 (requirements.txt)

```
neo4j>=5.0
rdflib>=7.0
owlrl>=6.0
anthropic>=0.116
```

---

## 참고 / 주의

- 현재 Neo4j 컨테이너에 **검증용 합성 데이터 80건**이 들어있음. 실데이터로 쓰려면
  `MATCH (n) DETACH DELETE n`로 비우고 `backfill.py` 실행.
- consumer의 dual-write는 완전 선택적 — Neo4j 없이도 기존 파이프라인 정상 동작.
- 이 문서는 임시 개발 정리. 정식 내용은 `README.md`의 "그래프 DB + 온톨로지 + LLM" 섹션 참고.
