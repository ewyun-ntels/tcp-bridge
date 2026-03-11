# TCP 브리지

TCP 브리지는 외부 TCP 서버와 내부 NATS 간 요청/응답을 중계하는 Go 애플리케이션입니다. 현재 구현은 우선순위 기반 다중 TCP 연결, NATS Queue Group 구독, 분리된 inbound/outbound 워커 풀, TID 기반 inflight 매칭을 사용합니다.

## 핵심 구조

- `ConnectionManager`: endpoint별 `ConnMgr`를 관리하고 READY 상태인 가장 낮은 priority 연결을 active로 선택합니다.
- `tcp.Reader`: 각 `ConnMgr.readLoop()`가 읽어온 프레임을 받아 request/response로 분기합니다.
- `BridgeHandler`: `OutboundWorkerPool`과 `InboundWorkerPool`을 소유하고 NATS/TCP 브리지 흐름을 조정합니다.
- `InflightManager`:
  - `InflightA`: NATS -> TCP RPC 요청 추적, key=`tid`
  - `InflightB`: TCP -> NATS 요청 추적, key=`connectionID:TID`
- `nats.Client`: `Subscriber`, `ReplyPublisher`, `Requester`를 같은 `nats.Conn` 위에서 제공합니다.
- `metrics.Metrics`: Prometheus 메트릭과 `/health`, `/keylist` HTTP 엔드포인트를 제공합니다.

## 런타임 흐름

### NATS -> TCP

1. `BridgeHandler.Start()`가 `message_type_routing.outbound`에 정의된 모든 subject를 queue group `tcp-bridge-workers`로 구독합니다.
2. 구독 콜백은 `BridgeHandler.HandleOutboundRequest()`를 호출하고, 메시지는 outbound 작업 큐에 적재됩니다.
3. `OutboundWorkerPool.process()`가 subject로부터 request/response 타입을 해석하고 `config.Frame`을 생성합니다.
4. RPC 요청이면 `InflightA.Register()`로 entry를 등록합니다.
5. `sendAndWaitWithRetry()`가 priority 순서대로 READY connection을 순회하면서 `retry_attempts` 만큼 전송/응답대기를 반복합니다.
6. TCP 응답은 `App.handleTCPResponse()` -> `InflightA.HandleResponse()`로 매칭되고, payload만 NATS reply subject로 발행됩니다.

### TCP -> NATS

1. 각 `ConnMgr.readLoop()`는 business frame을 읽어 `frame.ConnectionID`를 설정한 뒤 `tcp.Reader.handleFrame()`으로 전달합니다.
2. request 타입이면 `App.handleTCPRequest()` -> `FrameDispatcher.DispatchRequestFrame()` -> `BridgeHandler.HandleInboundFrame()` 순으로 inbound 작업 큐에 적재됩니다.
3. `InboundWorkerPool.process()`가 `message_type_routing.inbound`로부터 NATS subject와 TCP response type을 결정합니다.
4. `InflightB.Register()`로 lifecycle entry를 저장합니다.
5. `Requester.RequestWithTimeoutToSubject()`가 payload만 NATS로 보내고 응답을 기다립니다.
6. 응답이 오면 `sendTCPResponseToConnection()`이 원래 `ConnectionID`로만 TCP response frame을 전송합니다.
7. 실패 시 동일 connection으로 TCP 에러 응답을 시도하고 `InflightB.Remove()`로 정리합니다.

## 연결과 failover

- 각 endpoint는 독립적인 `connectionLoop()`를 갖고 재연결을 수행합니다.
- 상태는 `DISCONNECTED`, `CONNECTING`, `CONNECT_FAILED`, `READY`를 사용합니다.
- outbound(NATS -> TCP)는 READY connection 중 가장 낮은 priority부터 시도합니다.
- inbound(TCP -> NATS)의 응답은 failover 하지 않고 요청을 받은 원래 `ConnectionID`로만 보냅니다.
- `readLoop`, `pingLoop`, write error가 연결 이상을 감지하면 상태를 내리고 background reconnect가 다시 HELLO/ACK를 수행합니다.

## 설정

현재 구현은 `nats.message_type_routing.outbound`와 `inbound`를 별도로 사용합니다.

```yaml
server:
  name: "tcp-bridge"
  version: "1.0.0"
  environment: "development"
  shutdown_timeout: "3s"

tcp:
  endpoints:
    - host: "127.0.0.1"
      port: 8000
      priority: 0
    - host: "127.0.0.1"
      port: 7000
      priority: 1
    - host: "127.0.0.1"
      port: 7070
      priority: 2
  connect_timeout: "2s"
  write_timeout: "30s"
  keep_alive: "30s"
  max_frame_size: 65535
  frame_header_size: 8
  handshake_timeout: "10s"
  ping_timeout: "5s"
  sys_id: "tcp-bridge-01"
  branch_name: "SS"

nats:
  urls:
    - "nats://localhost:4222"
  connect_timeout: "5s"
  message_type_routing:
    outbound:
      "05":
        subject: "tcp.subs.change"
        response_msg_type: "06"
      "09":
        subject: "tcp.cellinfo.noti"
        response_msg_type: "0a"
    inbound:
      "07":
        subject: "tcp.subs.info"
        response_msg_type: "08"
      "0b":
        subject: "tcp.subs.sync"
        response_msg_type: "0c"

message_handler:
  outbound:
    response_timeout: "5s"
    retry_attempts: 10
    worker_count: 4
    queue_size: 128
  inbound:
    timeout: "30s"
    worker_count: 4
    queue_size: 128

metrics:
  enabled: true
  port: 8080
  path: "/metrics"

logging:
  level: "debug"
  format: "json"
```

## 프레임 프로토콜

- 헤더 크기: 8 bytes
- Byte 1: Extension Bit + Protocol Version + Reserved, 현재 `0x00`
- Byte 2: Message Type
- Byte 3-4: Body Length, big endian `uint16`
- Byte 5-8: Transaction ID, big endian `uint32`
- Byte 8+: JSON payload

주요 타입은 다음과 같습니다.

| Type | 의미 | 주 흐름 |
|---|---|---|
| `0x01` | HELLO request | handshake |
| `0x02` | HELLO response | handshake |
| `0x03` | PING request | keepalive |
| `0x04` | PONG response | keepalive |
| `0x05` | Subs-Change request | NATS -> TCP |
| `0x06` | Subs-Change response | TCP -> NATS reply path |
| `0x07` | Subs-Info request | TCP -> NATS |
| `0x08` | Subs-Info response | NATS -> TCP reply path |
| `0x09` | CellInfo-Noti request | NATS -> TCP |
| `0x0a` | CellInfo-Noti response | TCP -> NATS reply path |
| `0x0b` | Subs-Sync request | TCP -> NATS |
| `0x0c` | Subs-Sync response | NATS -> TCP reply path |

`Frame.ConnectionID`는 런타임 메타데이터이며 wire format에는 포함되지 않습니다.

## Handshake / Ping

연결 후 bridge는 HELLO(`0x01`)를 보내고 ACK(`0x02`)를 기다립니다.

HELLO payload:

```json
{
  "sys-id": "tcp-bridge-01",
  "branch-name": "SS"
}
```

ACK payload:

```json
{
  "sys-id": "tcp-server-01",
  "code": 200,
  "ping-interval": 30,
  "keyList": [],
  "cause": ""
}
```

- `code == 200`일 때 handshake 성공으로 간주합니다.
- `ping-interval`이 0보다 크면 READY 상태에서 `pingLoop()`가 idle 기반 ping/pong 감시를 수행합니다.
- PONG 미수신 또는 ping write 실패 시 연결은 `DISCONNECTED`로 내려갑니다.

## 메트릭

기본 엔드포인트:

- `/metrics`
- `/health`
- `/keylist`

대표 메트릭 범주:

- connection state / count
- TCP sent / received / bytes transmitted
- NATS processed count / request duration
- inflight entry / timeout
- worker task / error

참고로 일부 queue/semaphore 메트릭은 구조상 남아 있지만 현재 런타임 경로에서 적극적으로 갱신되지 않을 수 있습니다.

## 빌드와 실행

직접 실행:

```bash
go build -o tcp-bridge ./cmd/tcp-bridge
./tcp-bridge config.yaml
```

Make 사용:

```bash
make build-local
./bin/tcp-bridge config.yaml
```

Docker:

```bash
docker build -t tcp-bridge .
docker run --rm -it -p 8080:8080 -v "$(pwd)/config.yaml:/app/config.yaml:ro" tcp-bridge
```

## 프로젝트 구조

```text
.
├── cmd/tcp-bridge
├── docs
├── internal/config
├── internal/connection
├── internal/inflight
├── internal/metrics
├── internal/nats
├── internal/tcp
├── internal/tid
├── internal/worker
├── monitoring
└── simulator/simple
```

## 관련 문서

- `docs/MESSAGE_TYPE_FLOW.md`
- `tcp-bridge-codex.xml`
