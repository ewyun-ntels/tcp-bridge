# TCP 브리지

TCP 브리지는 Go로 구현된 고성능, 내결함성 TCP-to-NATS 메시지 브리지입니다. 외부 TCP 서버와 내부 NATS 클러스터 간의 양방향 메시지 라우팅을 제공하며, **VIP 절체 대응**, **Priority 기반 자동 Failover**, **재시도 로직**, 포괄적인 모니터링 기능을 포함합니다.

## 아키텍처

### 핵심 구성 요소

- **Connection Manager**: 우선순위 기반 다중 TCP 연결 관리 (최대 3개: Primary → Secondary → Tertiary)
- **Priority Selector**: 자동 Failover 로직 (Priority 0 READY → Priority 1 READY → Priority 2 READY)
- **TCP Sender**: 동시성 제어(Mutex)를 통한 직접 TCP 전송
- **Reader**: TCP 읽기 + 프레이밍 + frameType별 디스패치 (각 connection별 고루틴)
- **MessageHandler**: NATS Queue Group을 활용한 통합 메시지 처리기
- **Retry Logic**: VIP 절체 대응을 위한 지능형 재시도 (같은 connection에 N번 재시도 → 다음 priority로 Failover)
- **Inflight Managers**: 
  - **Inflight-A**: NATS→TCP 대기 중인 요청 (RPC만)
  - **Inflight-B**: TCP→NATS 대기 중인 요청 (TCP 인바운드는 항상)

### 메시지 플로우

#### NATS → TCP (Internal 방향) - VIP 절체 대응 재시도
1. **NATS Queue Group Subscriber**가 `external_req` subject로 메시지 수신 (로드 밸런싱)
2. **MessageHandler**가 메시지 처리 (여러 고루틴에서 병렬 처리)
3. TID 생성 및 TCP 프레임 생성
4. Inflight-A에 등록 (RPC만)
5. **Priority 기반 재시도 전략**:
   - Priority 0 (Primary): `retry_attempts`번 시도 (각 시도마다 `response_timeout` 대기)
   - 모두 실패 시 Priority 1 (Secondary)로 Failover하여 동일하게 재시도
   - Priority 2 (Tertiary)까지 모두 시도
6. TCP 응답 수신 시 **NATS Reply Publisher**가 응답 전송
7. **Core NATS Queue Group** 기반으로 여러 bridge 인스턴스 간 로드 밸런싱

#### TCP → NATS (External 방향)
1. **Reader**가 TCP REQUEST 프레임 수신 (각 connection별 고루틴)
2. **TCP Inbound Handler**가 **MessageHandler**로 직접 전달
3. **MessageHandler**가 메시지 처리
4. Inflight-B에 등록 (항상)
5. **NATS Requester**가 Message Type 기반 subject로 요청 전송 후 응답 대기
6. **TCP Sender**로 응답 직접 전송 (Mutex 보호)

### VIP 절체 대응 재시도 로직

VIP 절체 발생 시 안정적인 메시지 전송을 보장합니다:

```
Priority 0 (Primary VIP):
  0초:  1차 전송 → 성공 → response_timeout(5초) 대기
  5초:  응답 없음 → 2차 전송 → response_timeout 대기
  10초: VIP 절체 완료, 응답 수신! ✅

Priority 0 완전 다운:
  0~14초:  Priority 0에서 3번 시도 모두 실패
  14~28초: Priority 1 (Secondary)로 Failover → 2번째 시도에서 성공! ✅
  
모든 Priority 실패:
  0~42초: 모든 connection 시도 (3개 × 3번 × 7초)
  42초:   최종 timeout 에러 응답
```

### 동적 라우팅 (Message Type 기반)

TCP에서 NATS로 향하는 메시지는 **Message Type (4.1.1.d)** 에 따라 동적으로 subject가 결정됩니다:

```yaml
message_type_routing:
  mapping:
    "05": "tcp.subs.change"          # Subs-Change-Request
    "06": "tcp.subs.change.res"      # Subs-Change-Response
    "07": "tcp.subs.info"            # Subs-Info-Request
    "08": "tcp.subs.info.res"        # Subs-Info-Response
    "09": "tcp.cellinfo.noti"        # CellInfo-Noti-Request
    "0a": "tcp.cellinfo.noti.res"    # CellInfo-Noti-Response
    "0b": "tcp.subs.sync"            # Subs-Sync-Request
    "0c": "tcp.subs.sync.res"        # Subs-Sync-Response
  default_subject: "tcp.unknown"     # 매핑되지 않은 경우 기본 subject
```

## 주요 기능

### 1. VIP 절체 대응 재시도 로직 ⭐
- **같은 Connection 재시도**: VIP 절체 시간(2초) 고려하여 3번 재시도
- **Priority Failover**: Primary 실패 시 자동으로 Secondary → Tertiary 순차 시도
- **빠른 응답**: `response_timeout` (5초) 내 응답 없으면 즉시 재시도
- **최종 안정성**: 모든 connection 실패 시 명확한 timeout 응답

### 2. 우선순위 기반 다중 연결 관리
- **3단계 Priority**: Primary (0) → Secondary (1) → Tertiary (2)
- **자동 Failover**: 가장 낮은 priority 값의 READY connection 자동 선택
- **백그라운드 재연결**: 연결 끊김 시 5초 간격 자동 재연결 시도
- **상태 기반 선택**: READY 상태 connection만 활성화

#### 백그라운드 자동 재연결 메커니즘

ConnectionManager는 각 connection의 상태를 지속적으로 모니터링하며, 연결 끊김 발생 시 자동으로 재연결을 시도합니다.

**연결 끊김 감지 (3가지 경로)**

1. **Read 실패 감지** ([manager.go:545-555](internal/connection/manager.go#L545-L555))
   ```
   readLoop() → Read 실패 (EOF, Network Error) → setState(Disconnected)
   ```

2. **PING Timeout 감지** ([manager.go:789-795](internal/connection/manager.go#L789-L795))
   ```
   pingLoop() → PONG 미수신 감지 → setState(Disconnected)
   ```

3. **PING 전송 실패** ([manager.go:801-806](internal/connection/manager.go#L801-L806))
   ```
   pingLoop() → PING 전송 실패 → setState(Disconnected)
   ```

**자동 재연결 프로세스** ([manager.go:361-379](internal/connection/manager.go#L361-L379))

```
connectionLoop() (백그라운드 고루틴)
    ↓
5초마다 체크
    ↓
state == Disconnected?
    ↓ YES
attemptConnection()
    ↓
연결 시도 → Handshake → setState(READY)
```

**전체 흐름**

```
[연결 끊김 발생]
     ↓
readLoop/pingLoop가 즉시 감지
     ↓
setState(Disconnected)
     ↓
최대 5초 이내 connectionLoop가 감지
     ↓
attemptConnection() 호출
     ↓
TCP 연결 재시도 → Handshake → READY
```

### 3. NATS Queue Group 기반 로드 밸런싱
- **Queue Group 활용**: 여러 tcp-bridge 인스턴스 간 자동 로드 밸런싱
- **종료 시 안전성**: tcp-bridge 종료해도 NATS에 메시지 안전 보관
- **병렬 처리**: 각 메시지는 독립된 고루틴에서 처리

### 4. 고성능 직접 전송 처리
- NATS→TCP 방향: 직접 처리로 중간 큐 오버헤드 제거
- Mutex 기반 직접 TCP 전송으로 지연시간 최소화
- 메모리 효율적: 내부 큐 제거로 리소스 사용량 감소

### 5. Message Type 기반 동적 라우팅
- TCP 프레임의 Message Type 필드에 따라 NATS subject 자동 결정
- 설정 파일로 라우팅 룰 유연하게 관리
- 매핑되지 않은 타입은 default_subject로 전송

### 6. 프레임 기반 프로토콜
- 트랜잭션 ID(TID)가 있는 커스텀 TCP 프레이밍
- JSON 기반 Handshake: 시스템 ID 및 지점 정보를 포함한 연결 인증
- Ping/Pong 메커니즘: 서버 지정 주기에 따른 자동 연결 상태 확인

### 7. 포괄적인 모니터링
- 실시간 Prometheus 메트릭 수집 및 노출
- 연결 상태, 처리량, 오류율 등 모든 지표 추적
- 우아한 종료 및 리소스 정리

## 설치 및 실행

```bash
# 저장소 클론
git clone <repository-url>
cd tcp_bridge

# 애플리케이션 빌드
go build -o tcp-bridge ./cmd/tcp-bridge

# 설정 파일과 함께 실행
./tcp-bridge config.yaml
```

## 설정

전체 설정 옵션은 `config.yaml`을 참조하세요:

```yaml
# 서버 설정
server:
  name: "tcp-bridge"
  version: "1.0.0"
  environment: "development"
  shutdown_timeout: "3s"

# TCP 연결 설정 (우선순위 기반 다중 연결)
tcp:
  endpoints:
    - host: "127.0.0.1"
      port: 8000
      priority: 0  # Primary (최고 우선순위)
    - host: "192.168.1.101" 
      port: 8000
      priority: 1  # Secondary
    - host: "127.0.0.1"
      port: 7070
      priority: 2  # Tertiary
  
  connect_timeout: "5s"       # 연결 시도 timeout
  write_timeout: "30s"
  keep_alive: "30s"
  max_frame_size: 65535       # 64KB (프로토콜 스펙: uint16 최대값)
  handshake_timeout: "10s"
  ping_timeout: "5s"
  
  # Handshake credentials
  sys_id: "tcp-bridge-01"     # 접속하는 Peer Name
  branch_name: "SS"           # 국사명 (SS: 성수, DS: 둔산, BR: 보라매)

# NATS 설정
nats:
  urls: ["nats://localhost:4222"]
  connect_timeout: "5s"
  external_req_subject: "external_req"  # NATS→TCP subject
  
  # TCP→NATS 라우팅: Message Type 기반 (4.1.1.d)
  message_type_routing:
    mapping:
      "05": "tcp.subs.change"          # Subs-Change-Request
      "06": "tcp.subs.change.res"      # Subs-Change-Response
      "07": "tcp.subs.info"            # Subs-Info-Request
      "08": "tcp.subs.info.res"        # Subs-Info-Response
      "09": "tcp.cellinfo.noti"        # CellInfo-Noti-Request
      "0a": "tcp.cellinfo.noti.res"    # CellInfo-Noti-Response
      "0b": "tcp.subs.sync"            # Subs-Sync-Request
      "0c": "tcp.subs.sync.res"        # Subs-Sync-Response
    default_subject: "tcp.unknown"
  

# 메시지 처리 설정 (VIP 절체 대응)
message_handler:
  outbound:  # NATS→TCP processing
    response_timeout: "5s"    # 각 시도별 TCP 응답 대기 시간
    retry_attempts: 3         # 같은 connection에 재시도 횟수
    # 참고: 최대 소요 시간 = endpoints(3) × retry_attempts(3) × response_timeout(5s) ≈ 45초
  
  inbound:  # TCP→NATS processing
    timeout: "30s"
    retry_attempts: 3

# 메트릭 설정
metrics:
  enabled: true
  port: 8080
  path: "/metrics"

# 로깅 설정
logging:
  level: "info"    # debug, info, warn, error
  format: "json"   # json, text
```

## 프레임 프로토콜

TCP 프로토콜은 커스텀 프레임 포맷을 사용합니다 (4.1):

```
+----------+----------+----------+----------+---------+
| Ver (1)  | Type (1) | Len (2)  | TID (4)  | Payload |
+----------+----------+----------+----------+---------+
```

### 헤더 구조 (8 bytes)
- **Byte 1**: Extension Bit(1) + Protocol Version(2) + Reserved(5) = 0x00
- **Byte 2**: Message Type (1 byte)
- **Byte 3-4**: Body Length (2 bytes, Big Endian, uint16)
- **Byte 5-8**: Transaction Identifier (4 bytes, Big Endian, uint32)

### Message Types (4.1.1.d)
| Type | Code | Description |
|------|------|-------------|
| HELLO | 0x01 | Handshake 요청 |
| ACK | 0x02 | Handshake 응답 |
| PING | 0x03 | Ping (연결 상태 확인) |
| PONG | 0x04 | Pong (Ping 응답) |
| Subs-Change-Request | 0x05 | 가입자 변경 요청 |
| Subs-Change-Response | 0x06 | 가입자 변경 응답 |
| Subs-Info-Request | 0x07 | 가입자 정보 요청 |
| Subs-Info-Response | 0x08 | 가입자 정보 응답 |
| CellInfo-Noti-Request | 0x09 | Cell 정보 알림 요청 |
| CellInfo-Noti-Response | 0x0a | Cell 정보 알림 응답 |
| Subs-Sync-Request | 0x0b | 가입자 동기화 요청 |
| Subs-Sync-Response | 0x0c | 가입자 동기화 응답 |

### Transaction ID (TID) 생성

**NATS → TCP 방향** (새 요청):
- **자동 생성**: 전역 atomic counter로 thread-safe하게 생성
- **범위**: uint32 (1 ~ 4,294,967,295)
- **시작값**: 1 (0은 사용하지 않음)
- **일일 리셋**: 매일 자정(YYYYMMDD 변경 시) 1로 리셋
- **Overflow 처리**: uint32 최대값 초과 시 1로 재설정
- **전역 유니크**: 모든 고루틴에서 하나의 counter 공유

**TCP → NATS 방향** (응답):
- 수신한 TCP 프레임의 TID를 그대로 사용 (요청-응답 매칭용)

**프로토콜 메시지** (Handshake, Ping/Pong):
- 고정값 TID = 0 사용

## 연결 상태

각 연결 관리자는 다음 상태 중 하나를 유지합니다:
- `DISCONNECTED`: 활성 연결 없음
- `CONNECTING`: 연결 시도 중
- `READY`: 연결 성공, 트래픽 처리 준비 완료
- `CONNECT_FAILED`: 연결 실패 (재시도 중)

**Priority Selector**는 READY 상태인 connection 중 가장 낮은 priority 값을 가진 연결을 자동 선택합니다.

## 메트릭

`/metrics` 엔드포인트에서 포괄적인 Prometheus 메트릭을 제공합니다:

- **연결 상태 및 개수**: Priority별 connection 상태 추적
- **TCP 프레임 전송 통계**: 프레임 타입별, Priority별 전송 성공/실패
- **NATS 메시지 처리 메트릭**: Queue Group 메시지 처리량, ACK/NAK 비율
- **재시도 통계**: Priority별 재시도 횟수, Failover 발생 횟수
- **응답 시간**: 각 Priority별 평균 응답 시간
- **Inflight 요청 추적**: 진행 중인 요청 수 (Inflight-A, Inflight-B)
- **메시지 처리 성능**: 처리량, 지연시간, 에러율

## 아키텍처 다이어그램

```
[NATS 클러스터 (Queue Group)] ←→ [tcp-bridge] ←→ [외부 TCP 서버 (VIP / 3개 Priority)]
                               │
                               ├─ NATS Handler (Queue Group 기반 병렬 처리)
                               ├─ Connection Manager (Priority 0~2)
                               │  ├─ ConnMgr Priority 0 (Primary)
                               │  ├─ ConnMgr Priority 1 (Secondary)
                               │  └─ ConnMgr Priority 2 (Tertiary)
                               ├─ Priority Selector (자동 Failover)
                               ├─ Retry Logic (VIP 절체 대응)
                               ├─ TCP Sender (Mutex 보호, 직접 전송)
                               ├─ TCP Reader (각 connection별 고루틴)
                               ├─ Inflight-A/B Managers (요청 추적)
                               └─ TCP Inbound Handler (Message Type 라우팅)
```

## 개발

### 프로젝트 구조
```
tcp_bridge/
├── cmd/tcp-bridge/     # 메인 애플리케이션
├── internal/
│   ├── config/         # 설정 및 타입
│   ├── connection/     # 연결 관리
│   ├── inflight/       # 진행 중인 요청 추적
│   ├── metrics/        # Prometheus 메트릭
│   ├── nats/          # NATS 클라이언트 구성 요소
│   ├── semaphore/     # 동시성 제어
│   ├── tcp/           # TCP sender/reader (직접 전송)
│   └── worker/        # 메시지 처리기 및 디스패처
└── config.yaml        # 설정 파일
```

## Handshake 프로토콜

tcp-bridge는 TCP 서버와 연결 시 JSON 기반 handshake를 수행합니다.

### HELLO 메시지 (클라이언트 → 서버, Type: 0x01)

```json
{
  "sys-id": "tcp-bridge-01",
  "branch-name": "SS"
}
```

- `sys-id`: 접속하는 Peer Name
- `branch-name`: 국사명 (SS: 성수, DS: 둔산, BR: 보라매)

### ACK 메시지 (서버 → 클라이언트, Type: 0x02)

```json
{
  "sys-id": "tcp-server-01",
  "code": 0,
  "ping-interval": 30,
  "keyList": [],
  "cause": ""
}
```

- `sys-id`: 응답하는 Peer Name
- `code`: 결과 코드 (0=성공)
- `ping-interval`: Ping 메시지 전송 주기 (초 단위)
- `keyList`: 암호화 키 목록 (선택사항)
- `cause`: 실패 시 상세 사유 (선택사항)

## Ping/Pong 메커니즘

Handshake 완료 후, tcp-bridge는 서버로부터 받은 `ping-interval` 주기에 따라 자동으로 PING 메시지를 전송합니다.

```
tcp-bridge ──PING(0x03)──→ TCP Server
tcp-bridge ←─PONG(0x04)─── TCP Server
```

- PING/PONG 프레임은 JSON 페이로드 없이 헤더만 전송
- TID는 0으로 고정
- 연결 상태 확인 및 유지에 사용
- `ping_timeout` (5초) 내 PONG 미수신 시 연결 종료 후 재연결

### 빌드

#### 빠른 시작 (권장)

```bash
# 자동화된 빠른 시작 스크립트
./quick-start.sh
```

#### Make를 사용한 빌드

```bash
# 도움말 보기
make help

# 로컬 개발용 빌드
make build-local

# Linux용 릴리즈 빌드
make build

# 테스트 실행
make test

# 코드 포맷팅 및 린팅
make fmt lint

# 의존성 관리
make deps
```

#### 수동 빌드

```bash
go mod tidy
go build -o tcp-bridge ./cmd/tcp-bridge
```

### Docker

#### Docker 이미지 빌드

```bash
# Make를 사용한 빌드 (권장)
make docker-build

# 수동 빌드
docker build -t tcp-bridge .
```

#### Docker 실행

```bash
# Make를 사용한 실행
make docker-run

# 수동 실행
docker run --rm -it \
  -p 8080:8080 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  tcp-bridge
```

### 실행

```bash
# 기본 설정으로 실행
./tcp-bridge

# 커스텀 설정으로 실행
./tcp-bridge /path/to/config.yaml
```

## 주요 특징 상세

### 1. NATS Queue Group 기반 메시지 처리
- **Queue Group 활용**: 여러 tcp-bridge 인스턴스 간 자동 로드 밸런싱
- **종료 시 안전성**: tcp-bridge 종료해도 NATS에 메시지 안전 보관
- **병렬 처리**: 각 메시지는 독립된 고루틴에서 동시 처리

### 2. VIP 절체 대응 재시도 로직
- **같은 Connection 재시도**: 지정 횟수만큼 즉시 재시도
- **빠른 Failover**: response_timeout (5초) 내 응답 없으면 즉시 재시도
- **Priority 순차 시도**: Primary (3번) → Secondary (3번) → Tertiary (3번)
- **자동 재연결 대기**: ConnectionManager가 백그라운드에서 5초 간격 재연결

### 3. 우선순위 기반 다중 연결 관리
- **3단계 Priority**: Primary (0) → Secondary (1) → Tertiary (2)
- **자동 선택**: 가장 낮은 priority 값의 READY connection 자동 활성화
- **상태 모니터링**: 각 connection의 DISCONNECTED/CONNECTING/READY 상태 추적
- **투명한 Failover**: 상위 priority 복구 시 자동으로 다시 활성화

### 4. 고성능 직접 전송
- 중간 큐 제거로 지연시간 최소화
- Mutex 기반 동시성 제어로 프레임 경계 보호
- 메모리 효율적 설계

### 5. Message Type 기반 동적 라우팅
- TCP 프레임의 Message Type (Byte 2)에 따라 NATS subject 자동 결정
- Hex 매핑으로 유연한 라우팅 설정
- 매핑되지 않은 타입은 default_subject로 안전하게 처리

### 6. 포괄적인 모니터링
- Priority별 connection 상태 및 재시도 통계
- 응답 시간, 처리량, 에러율 실시간 추적
- Prometheus 메트릭으로 시각화 및 알림 가능

## 메시지 라우팅 예시

### TCP → NATS 라우팅 (Message Type 기반)
```
입력: TCP Frame (Type: 0x05)
      Message Type: Subs-Change-Request
출력: NATS Subject "tcp.subs.change"로 전송

입력: TCP Frame (Type: 0x09)
      Message Type: CellInfo-Noti-Request
출력: NATS Subject "tcp.cellinfo.noti"로 전송

입력: TCP Frame (Type: 0xff - 매핑 없음)
출력: NATS Subject "tcp.unknown"로 전송 (기본값)
```

### NATS → TCP 라우팅
```
NATS "external_req" subject 수신
  ↓
Priority 0 (Primary): 3번 시도 (각 5초 대기, 2초 간격)
  ↓ 실패 시
Priority 1 (Secondary): 3번 시도
  ↓ 실패 시
Priority 2 (Tertiary): 3번 시도
  ↓ 모두 실패
Timeout 에러 응답 전송
```
