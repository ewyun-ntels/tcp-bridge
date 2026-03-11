# Simple Simulator

`simulator/simple`만 사용하면 됩니다.

파일:
- `tcp-server.go`: tcp-bridge와 TCP 연결, HELLO/ACK, ping-pong 유지. 주기적으로 TCP REQUEST를 보내고 RESPONSE를 로그로 출력하며, 반대로 들어오는 TCP REQUEST에도 응답합니다.
- `nats-client-request.go`: NATS request를 보내고 응답을 받습니다.
- `nats-client-responder.go`: NATS request를 받아 응답합니다.

예시:

```bash
go run tcp-server.go -port 8000 -request-interval 3s
```

```bash
go run nats-client-request.go -subject tcp.subs.change -interval 2s
```

```bash
go run nats-client-responder.go -subject tcp.subs.info
```

주요 옵션:
- `tcp-server.go`
  - `-port`: TCP listen 포트
  - `-request-interval`: TCP request 전송 주기
  - `-request-count`: 전송 개수, `0`이면 무한
  - `-request-type`: 전송할 TCP request frame type, 기본값 `07`
- `nats-client-request.go`
  - `-url`: NATS URL
  - `-subject`: request subject
  - `-interval`: 전송 주기
  - `-timeout`: 응답 대기 시간
  - `-count`: 전송 개수, `0`이면 무한
- `nats-client-responder.go`
  - `-url`: NATS URL
  - `-subject`: subscribe subject
  - `-name`: 응답자 이름
