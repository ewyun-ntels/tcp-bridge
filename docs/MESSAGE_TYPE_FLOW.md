# 메시지 타입 처리 가이드

이 문서는 현재 구현 기준의 message type 라우팅과 실제 처리 경로를 설명합니다. 기준 소스는 `internal/config`, `internal/tcp`, `internal/worker`, `internal/inflight`입니다.

## 메시지 타입

| 값 | 이름 | 요청 방향 | 응답 방향 |
|---|---|---|---|
| `0x01` | HELLO request | bridge -> TCP server | handshake |
| `0x02` | HELLO response | TCP server -> bridge | handshake |
| `0x03` | PING request | bridge -> TCP server | keepalive |
| `0x04` | PONG response | TCP server -> bridge | keepalive |
| `0x05` | Subs-Change request | NATS -> TCP | `0x06` |
| `0x06` | Subs-Change response | TCP -> NATS reply | from `0x05` |
| `0x07` | Subs-Info request | TCP -> NATS | `0x08` |
| `0x08` | Subs-Info response | NATS -> TCP reply | from `0x07` |
| `0x09` | CellInfo-Noti request | NATS -> TCP | `0x0a` |
| `0x0a` | CellInfo-Noti response | TCP -> NATS reply | from `0x09` |
| `0x0b` | Subs-Sync request | TCP -> NATS | `0x0c` |
| `0x0c` | Subs-Sync response | NATS -> TCP reply | from `0x0b` |

Hello/Ping 계열은 `ConnectionManager`가 직접 처리하며 NATS 라우팅 테이블에 넣지 않습니다.

## 설정 형식

현재 구현은 `message_type_routing.outbound`와 `message_type_routing.inbound`를 분리합니다.

```yaml
nats:
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
```

- outbound map key: NATS -> TCP request type
- inbound map key: TCP -> NATS request type
- `response_msg_type`: 기대하거나 생성할 TCP response type

## NATS -> TCP 흐름

대상 타입: `0x05`, `0x09`

1. `BridgeHandler.Start()`가 `GetOutboundSubjects()`로 subject 목록을 얻습니다.
2. `Subscriber.SubscribeSubjectsWithQueue(subjects, "tcp-bridge-workers", ...)`가 각 subject를 queue group으로 구독합니다.
3. NATS 메시지가 들어오면 `HandleOutboundRequest()`가 outbound 작업 큐에 적재합니다.
4. `OutboundWorkerPool.process()`는 `resolveOutboundRoute(subject)`로 request/response type을 구합니다.
5. `tid.Next()`로 TID를 만들고 `config.Frame{Type,TID,Payload}`를 생성합니다.
6. RPC 요청이면 `InflightA.Register()`가 entry를 생성합니다.
7. `sendAndWaitWithRetry()`는 priority 순서대로 READY connection을 순회합니다.
8. 응답이 오면 `InflightA.HandleResponse()`가 type / connectionID를 검증하고 `ResponseChan`으로 전달합니다.
9. `ReplyPublisher.PublishReply()`는 TCP response의 payload만 NATS reply subject에 전송합니다.

중요한 점:

- 구독 subject는 단일 `external_req`가 아니라 `message_type_routing.outbound[*].subject` 목록입니다.
- request 타입은 payload에서 추출하지 않고 NATS subject에서 역으로 결정합니다.
- RPC가 아닌 fire-and-forget 메시지는 `msg.Reply == ""`로 구분합니다.

## TCP -> NATS 흐름

대상 타입: `0x07`, `0x0b`

1. `ConnMgr.readLoop()`가 frame을 읽고 `frame.ConnectionID`를 채웁니다.
2. `tcp.Reader.handleFrame()`는 `MessageTypeRouting.IsRequestType()` / `IsResponseType()`로 분기합니다.
3. request 타입은 `App.handleTCPRequest()` -> `FrameDispatcher.DispatchRequestFrame()` -> `HandleInboundFrame()`으로 전달됩니다.
4. `InboundWorkerPool.process()`는 `GetInboundSubjectForMessageType(frame.Type)`로 NATS subject를 결정합니다.
5. `GetResponseType(frame.Type)`로 TCP 응답 타입을 계산합니다.
6. `InflightB.Register(frame.TID, TCPReplyInfo{ConnectionID, FrameType}, frame.Payload, deadline)`로 lifecycle entry를 등록합니다.
7. `Requester.RequestWithTimeoutToSubject(subject, frame.Payload, timeout)`가 payload만 NATS로 보냅니다.
8. 응답 성공 시 `sendTCPResponseToConnection(connectionID, tid, responseType, response)`를 호출합니다.
9. 실패 시 같은 connection으로 `sendTCPErrorResponseToConnection(...)`를 시도합니다.
10. 완료 후 `InflightB.Remove(connectionID, tid)`로 정리합니다.

중요한 점:

- TCP -> NATS 응답은 active connection으로 failover 하지 않습니다.
- 요청을 받은 원래 `ConnectionID`가 READY가 아니면 응답 전송이 실패합니다.
- `InflightB.ResponseChan`은 구조상 존재하지만 현재 `InboundWorkerPool.process()`는 동기 `RequestWithTimeoutToSubject()` 경로를 사용합니다.

## 분기 규칙

`tcp.Reader.handleFrame()`의 business frame 분기 규칙:

- `routing.IsRequestType(frame.Type)`이면 request path
- `routing.IsResponseType(frame.Type)`이면 response path
- 그 외 타입은 warning log 후 drop

`FrameDispatcher.DispatchRequestFrame()`는 request type인지 다시 확인한 후 inbound queue로 넘깁니다.

## Inflight 매칭

### InflightA

- key: `tid`
- 사용 경로: NATS -> TCP RPC
- 검증 항목:
  - 보낸 `ConnectionID`와 응답 `ConnectionID` 일치 여부
  - 기대한 `ExpectedResponseType`와 실제 response type 일치 여부

### InflightB

- key: `connectionID:TID`
- 사용 경로: TCP -> NATS
- 목적: 원래 TCP connection 기준 lifecycle 추적

같은 TID라도 connection이 다르면 서로 충돌하지 않습니다.

## 테스트 예시

NATS -> TCP:

```bash
nats req tcp.subs.change '{"subscriber":"A"}'
nats req tcp.cellinfo.noti '{"cellid":"12345"}'
```

TCP -> NATS:

```text
Header: [0x00][0x07][LEN][TID]
Body:   {"subscriber":"A"}
```

이 요청은 `tcp.subs.info` subject로 payload만 전달되고, 응답은 `0x08` frame으로 원래 connection에 기록됩니다.

## 관련 파일

- `internal/config/config.go`
- `internal/config/types.go`
- `internal/tcp/tcp.go`
- `internal/worker/bridge_handler.go`
- `internal/worker/outbound_pool.go`
- `internal/worker/inbound_pool.go`
- `internal/inflight/manager.go`
