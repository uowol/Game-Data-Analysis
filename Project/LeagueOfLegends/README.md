# ✨ **리그 오브 레전드 데이터 분석 프로젝트**

## **1️⃣ 프로젝트 개요**
**목표:**
- **이탈자 분석 및 이상 탐지**를 위한 대시보드 구축
- **매일 데이터를 갱신하는 배치 기반 분석 시스템**
- **실시간 모니터링보다는 게임 데이터 분석 및 시각화를 통해 비즈니스 인사이트를 제공하는 것에 중점**
- **데이터 수집 → 처리 → 저장 → 분석 → 시각화까지 전체 데이터 파이프라인 구축**

---

## **2️⃣ 고민했던 사항 및 해결 과정**

### **🔹 고민 1: 실시간 모니터링 vs 배치 기반 대시보드 (Grafana vs Metabase)**
#### **문제:**
- 초기에는 **실시간 모니터링(Grafana)을 고려했으나**, 주 목표가 실시간 지표 추적이 아닌 **게임 데이터 분석 및 인사이트 도출**임.
- **실시간 스트리밍보다는 배치 처리된 데이터를 활용하여 데이터 기반 의사결정을 내리는 것이 더 중요**함.
- **배치 분석을 위한 SQL 기반 시각화 도구로 Metabase를 고려**.

#### **결정:** 🚀 **Grafana 대신 Metabase 사용**
✅ **이유:**
- **Metabase는 비즈니스 인사이트 도출을 위한 EDA(탐색적 데이터 분석) 및 BI 분석에 적합**.
- **SQL 기반 쿼리 작성이 가능하고, 대시보드를 통해 여러 시점의 데이터를 직관적으로 분석 가능**.
- **Grafana는 실시간 모니터링에 적합하지만, 본 프로젝트에서는 필요하지 않음**.

---

### **🔹 고민 2: 메시지 큐 선택 (RabbitMQ vs Redis Streams)**
#### **문제:**
- Riot API에서 가져온 데이터를 처리할 메시지 큐 시스템이 필요함.
- **Redis Streams**를 사용하면 **RabbitMQ를 제거하고 Redis 하나로 해결 가능하지만, 자동 재처리가 어려움**.
- **RabbitMQ는 AMQP 기반으로 신뢰성이 높고 자동 재처리(재전송)가 가능**하지만, 중복 데이터 필터링이 필요함.

#### **결정:** 🚀 **RabbitMQ 선택**
✅ **이유:**
- **자동 재처리 지원 (`basic_nack(requeue=True)`)** → 실패 시 메시지를 자동으로 다시 큐에 넣을 수 있음.
- **메시지 지속성(`delivery_mode=2`) 설정으로 유실 방지 가능**.
- Redis Streams를 사용할 경우 **수동으로 `XPENDING + XCLAIM`을 관리해야 하는 부담이 큼**.

---

### **🔹 고민 3: 중복 데이터 필터링 (이미 저장된 데이터 재수집 방지)**
#### **문제:**
- **RabbitMQ는 단순한 메시지 큐이므로 중복 데이터 필터링 기능이 없음**.
- **이미 저장된 match_id를 다시 요청하는 것을 방지해야 함**.

#### **결정:** 🚀 **Redis를 활용한 중복 데이터 필터링 구현**
✅ **이유:**
- **RabbitMQ에서 메시지를 가져오기 전에 Redis에서 match_id를 체크**.
- **새로운 match_id만 DuckDB에 저장하도록 구현**.

📌 **구현 방식:**
```python
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

def is_duplicate(match_id):
    return r.sismember("processed_matches", match_id)

def mark_as_processed(match_id):
    r.sadd("processed_matches", match_id)
```

---

### **🔹 고민 4: 메시지 유실 방지 (RabbitMQ 메시지 지속성 설정)**
#### **문제:**
- 기본적으로 RabbitMQ는 **Consumer가 메시지를 가져가면 삭제되므로, Consumer가 죽으면 메시지가 유실될 위험이 있음**.

#### **결정:** 🚀 **RabbitMQ의 Durable Queue + Persistent 메시지 설정 적용**
✅ **이유:**
- **`queue_declare(durable=True)`** 설정을 통해 큐가 유지되도록 구성.
- **`delivery_mode=2`** 설정으로 메시지를 디스크에 저장하여 휘발성 방지.

📌 **구현 방식:**
```python
import pika

channel.queue_declare(queue='task_queue', durable=True)

channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(delivery_mode=2)  # Persistent 메시지 설정
)
```

---

### **🔹 고민 5: 배치 분석 시스템 구성 (DuckDB vs ClickHouse)**
#### **문제:**
- **실시간 스트리밍 처리가 필요한가, 아니면 배치 처리로 충분한가?**
- ClickHouse를 사용하면 **실시간 분석이 가능하지만, 500GB 저장 제한을 고려해야 함**.
- **매일 데이터가 갱신되는 배치 분석 시스템이라면 ClickHouse가 불필요할 수도 있음**.

#### **결정:** 🚀 **DuckDB(Parquet) 기반의 배치 분석 시스템 구축**
✅ **이유:**
- **매일 업데이트되는 배치 분석이므로 실시간 처리가 불필요**.
- **DuckDB의 컬럼 저장 방식(Parquet) 활용하여 데이터 저장 최적화**.
- **Metabase에서 쿼리 및 분석을 수행하여 데이터 탐색 및 인사이트 도출**.

---

## **3️⃣ 최종 기술 스택**

| 구성 요소 | 사용 기술 | 역할 |
|------------|------------|----------------------------|
| **데이터 수집** | Riot API | LoL 데이터 가져오기 |
| **메시지 처리** | RabbitMQ | 데이터 스트리밍 및 ETL 큐잉 |
| **중복 필터링** | Redis | match_id 중복 체크 및 캐싱 |
| **스케줄링 및 배치 처리** | Airflow | 매일 배치 작업 실행 |
| **데이터 저장** | DuckDB (Parquet) | DWH, 데이터 압축 및 최적화 |
| **모델 서빙** | MLflow + MinIO + FastAPI | 이상 탐지 모델 서빙 |
| **대시보드** | Metabase | 데이터 분석 및 시각화 |

---

## **4️⃣ EDA(탐색적 데이터 분석) 진행 단계**
✅ **EDA 진행 시점:**
1. **데이터 수집 및 저장이 완료된 후 실행** → API에서 충분한 데이터 확보.
2. **DuckDB에 적재된 데이터를 기본적으로 정리한 후 분석**.
3. **이탈자 및 이상 탐지 기준을 설정하기 전에 수행**.

✅ **EDA 주요 단계:**
- **데이터 요약 및 통계 분석** (결측값, 이상치 확인)
- **이탈자 및 이상 탐지 패턴 분석**
- **시각화를 활용한 데이터 분포 확인**

---

## **5️⃣ 최종 결론**

✅ **RabbitMQ를 사용하여 안정적인 메시지 큐 시스템을 구축**

✅ **Redis를 활용한 중복 데이터 필터링으로 불필요한 API 요청 방지**

✅ **RabbitMQ의 Durable Queue 설정을 적용하여 메시지 유실 방지**

✅ **매일 업데이트되는 배치 분석 방식이므로 DuckDB(Parquet) 활용**

✅ **Metabase를 활용하여 데이터 탐색 및 분석을 수행**

✅ **EDA를 데이터 수집 후 적절한 시점에서 실행하여 분석 신뢰성 확보**


