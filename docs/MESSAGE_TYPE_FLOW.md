# 메시지 타입 처리 가이드

## 메시지 타입 정의

TCP Bridge는 다음과 같은 메시지 타입을 처리합니다:

| 값   | 의미                        | 방향          | 용도          |
|------|-----------------------------|---------------|---------------|
| 0x01 | Hello-Request               | TCP 연결 관리 | Handshake     |
| 0x02 | Hello-Response              | TCP 연결 관리 | Handshake     |
| 0x03 | Ping-Request                | TCP 연결 관리 | Keepalive     |
| 0x04 | Ping-Response               | TCP 연결 관리 | Keepalive     |
| 0x05 | Subs-Change-Request         | NATS → TCP    | 비즈니스      |
| 0x06 | Subs-Change-Response        | TCP → NATS    | 비즈니스      |
| 0x07 | Subs-Info-Request           | TCP → NATS    | 비즈니스      |
| 0x08 | Subs-Info-Response          | NATS → TCP    | 비즈니스      |
| 0x09 | CellInfo-Noti-Request       | NATS → TCP    | 비즈니스      |
| 0x0a | CellInfo-Noti-Response      | TCP → NATS    | 비즈니스      |
| 0x0b | Subs-Sync-Request           | TCP → NATS    | 비즈니스      |
| 0x0c | Subs-Sync-Response          | NATS → TCP    | 비즈니스      |

**참고**: Hello와 Ping은 TCP 연결 레벨에서 직접 처리되며 NATS 라우팅을 거치지 않습니다.

## 메시지 플로우

### 1. NATS → TCP 방향

**요청 타입**: `0x05`, `0x09` (비즈니스 메시지)

```
┌──────┐                  ┌────────────┐                  ┌──────────┐
│ NATS │ ─── Request ───> │ TCP Bridge │ ─── Request ───> │ TCP Srv  │
│      │                  │            │                  │          │
│      │ <── Payload ──── │            │ <── Response ─── │          │
└──────┘                  └────────────┘                  └──────────┘
```

#### 처리 로직

1. **요청 수신**: NATS에서 `external_req` subject로 요청 수신
2. **메시지 타입 결정**: 
   - Payload에서 `msg_type` 필드 추출 (있는 경우)
   - 없으면 기본값 `0x05` 사용
3. **TCP 전송**: 
   - 헤더 생성 (8 bytes): `[Extension|Type|Length|TID]`
   - 헤더 + Payload를 TCP로 전송
4. **응답 처리**: 
   - TCP로부터 응답 프레임 수신
   - **Payload만 추출** (헤더 제거)
   - NATS reply subject로 전송

**참고**: Hello(0x01), Ping(0x03)은 TCP 연결 관리용이므로 이 플로우를 따르지 않습니다.

#### 코드 위치

- Handler: [`internal/worker/bridge_handler.go::HandleOutboundRequest`](../internal/worker/bridge_handler.go)
- Response 전송: [`internal/worker/outbound_pool.go::sendAndWaitWithRetry`](../internal/worker/outbound_pool.go)

```go
// TCP 응답의 payload만 NATS로 전송
replyPublisher.PublishReply(inflightEntry.ReplySubject, responseFrame.Payload)
```

---

### 2. TCP → NATS 방향

**요청 타입**: `0x07`, `0x0b`

```
┌──────────┐                  ┌────────────┐                  ┌──────┐
│ TCP Srv  │ ─── Request ───> │ TCP Bridge │ ─── Payload ───> │ NATS │
│          │                  │            │                  │      │
│          │ <── Response ─── │            │ <── Payload ──── │      │
└──────────┘                  └────────────┘                  └──────┘
```

#### 처리 로직

1. **요청 수신**: TCP에서 프레임 수신
   - 헤더 파싱: `[Extension|Type|Length|TID]`
   - Payload 추출
2. **NATS 라우팅**: 
   - Message Type을 기준으로 NATS subject 결정
   - `0x07` → `tcp.subs.info`
   - `0x0b` → `tcp.subs.sync`
3. **NATS 요청**: 
   - **Payload만 NATS로 전송** (헤더 제거)
   - Request/Response 패턴으로 대기
4. **응답 처리**: 
   - NATS로부터 응답 수신 (payload)
   - 응답 타입 계산: `요청 타입 + 1`
     - `0x07` → `0x08`
     - `0x0b` → `0x0c`
   - **헤더 생성** + Payload를 TCP로 전송

#### 코드 위치

- Handler: [`internal/worker/bridge_handler.go::HandleInboundFrame`](../internal/worker/bridge_handler.go)
- NATS 라우팅: [`internal/config/config.go::GetInboundSubjectForMessageType`](../internal/config/config.go)
- Response 전송: [`internal/worker/bridge_common.go::sendTCPResponseToConnection`](../internal/worker/bridge_common.go)

```go
// NATS 응답을 TCP로 전송 시 헤더 포함
responseFrame := &config.Frame{
    Type:    responseType,  // 요청 타입 + 1
    TID:     tid,
    Payload: response,      // NATS 응답
}
```

---

## 설정 파일

### config.yaml

```yaml
nats:
  external_req_subject: "external_req"  # NATS→TCP 요청 subject
  
  message_type_routing:
    mapping:
      # NATS → TCP 방향 (참고용)
      "01": "tcp.hello"
      "03": "tcp.ping"
      "05": "tcp.subs.change"
      "09": "tcp.cellinfo.noti"
      
      # TCP → NATS 방향 (실제 라우팅)
      "07": "tcp.subs.info"          # Subs-Info-Request
      "0b": "tcp.subs.sync"          # Subs-Sync-Request
```

---

## 주요 함수

### Frame 검증 메서드

```go
// types.go

// NATS→TCP 요청인지 확인
func (f *Frame) IsNATSToTCPRequest() bool {
    return f.Type == 0x01 || f.Type == 0x03 || 
           f.Type == 0x05 || f.Type == 0x09
}

// TCP→NATS 요청인지 확인
func (f *Frame) IsTCPToNATSRequest() bool {
    return f.Type == 0x07 || f.Type == 0x0b
}

// 응답 타입 계산
func (f *Frame) GetResponseType() uint8 {
    if f.IsRequest() {
        return f.Type + 1
    }
    return f.Type
}
```

---

## 테스트 시나리오

### NATS → TCP 테스트

```bash
# 1. Subs-Change-Request (0x05) 전송
nats req external_req '{"msg_type":"05","data":"test"}'

# 2. CellInfo-Noti-Request (0x09) 전송
nats req external_req '{"msg_type":"09","cellid":"12345"}'
```

### TCP → NATS 테스트

```bash
# 1. TCP에서 Subs-Info-Request (0x07) 전송
# TCP Server가 다음 프레임을 전송:
# Header: [0x00][0x07][LEN][TID]
# Body:   {"subscriber":"test"}

# 2. NATS subject 'tcp.subs.info'에서 요청 수신 확인
nats sub tcp.subs.info

# 3. 응답 전송 (payload만)
# TCP Bridge가 0x08 타입으로 헤더 포함하여 TCP로 전송
```

---

## 문제 해결

### NATS 응답에 헤더가 포함되는 경우

**증상**: NATS 클라이언트가 헤더 데이터를 받음

**원인**: `HandleOutboundRequest`에서 전체 프레임을 전송

**해결**: Line 289 확인 - `responseFrame.Payload`만 전송하는지 확인

### TCP 응답에 헤더가 없는 경우

**증상**: TCP 서버가 JSON만 받음

**원인**: `HandleInboundFrame`에서 payload만 전송

**해결**: `sendTCPResponseToConnection`이 호출되는지 확인 - Frame 전체를 전송해야 함

---

## 참고 자료

- [프레임 구조](../internal/config/types.go) - `Frame` 타입 정의
- [NATS 클라이언트](../internal/nats/client.go) - NATS 통신 로직
- [TCP 통신](../internal/tcp/tcp.go) - TCP 송수신 로직
- [메시지 핸들러](../internal/worker/bridge_handler.go) - 메시지 처리 로직
