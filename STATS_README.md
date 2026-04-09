# TCP Bridge Metrics

TCP Bridge에서 사용하는 Prometheus 메트릭을 정리한 문서입니다.

## Sections

- Connection Metrics
- Inbound Traffic Quality Metrics
- Outbound Traffic Quality Metrics

---

## Connection Metrics

### `tcp_bridge_connection_state`
- Type: `gauge`
- Labels: `connection_id`, `endpoint`
- Meaning: 각 TCP 연결의 현재 상태입니다.
- Values:
  - `0`: disconnected — 연결 끊김
  - `1`: connecting — 재연결 시도 중 (HELLO 전송 중)
  - `2`: ready — 연결됨

### `tcp_bridge_selected_connection`
- Type: `gauge`
- Labels: `connection_id`
- Meaning: 현재 outbound 전송에 사용 중인 primary/master connection 여부입니다.
- Values:
  - `1`: 현재 선택됨
  - `0`: 선택되지 않음

**Connection Operational Notes**
- `connection_state=1` 인 connection이 있으면 해당 회선이 재연결을 시도 중입니다.
- `connection_state=2` 이고 `selected_connection=1` 인 connection이 현재 active primary입니다.
- 모든 connection이 `connection_state=0` 이면 outbound 전송이 불가한 상태입니다.

---

## Inbound Traffic Quality Metrics

> Flow: TCP Server → TCP_BRIDGE → NATS (request) / NATS → TCP_BRIDGE → TCP Server (response)

### `tcp_bridge_inbound_requests_total`
- Type: `counter`
- Labels: `connection_id`, `msg_type`
- Meaning: TCP에서 수신한 inbound request 총 개수입니다.

### `tcp_bridge_inbound_responses_total`
- Type: `counter`
- Labels: `connection_id`, `msg_type`, `status`
- Meaning: inbound 요청의 최종 처리 결과 총 개수입니다.
- `status` values:
  - `success`: NATS 응답을 받아 TCP로 정상 응답
  - `error`: NATS 오류 또는 TCP 응답 전송 실패
  - `timeout`: NATS 응답 대기 시간 초과
  - `dropped`: worker queue 포화 또는 알 수 없는 msg_type

### `tcp_bridge_inbound_end_to_end_duration_seconds`
- Type: `histogram`
- Labels: `connection_id`, `msg_type`, `status`
- Meaning: TCP 요청 수신부터 TCP 응답 전송 완료까지의 전체 처리 시간입니다.

**Inbound Operational Notes**
- 소통률: `rate(inbound_responses_total{status="success"}[5m]) / rate(inbound_requests_total[5m])`
- `status="timeout"` 비율이 높으면 NATS 또는 downstream 서비스 응답 지연을 먼저 확인하세요.
- `status="dropped"` 가 발생하면 worker queue size 또는 worker count 조정이 필요할 수 있습니다.

---

## Outbound Traffic Quality Metrics

> Flow: NATS → TCP_BRIDGE → TCP Server (request) / TCP Server → TCP_BRIDGE → NATS (response)

### `tcp_bridge_outbound_requests_total`
- Type: `counter`
- Labels: `connection_id`, `subject`, `msg_type`
- Meaning: NATS에서 수신한 outbound request 총 개수입니다.
- Note: 연결 선택 전 단계에서 수신되므로 일부 시계열은 `connection_id="unknown"` 으로 기록될 수 있습니다.

### `tcp_bridge_outbound_responses_total`
- Type: `counter`
- Labels: `connection_id`, `subject`, `msg_type`, `status`
- Meaning: outbound 요청의 최종 처리 결과 총 개수입니다.
- `status` values:
  - `success`: TCP 응답을 받아 NATS로 정상 reply
  - `error`: TCP 전송 실패 또는 NATS reply 발행 실패
  - `timeout`: TCP 응답 대기 시간 초과 (모든 retry/priority 소진)
  - `dropped`: 알 수 없는 NATS subject

### `tcp_bridge_outbound_end_to_end_duration_seconds`
- Type: `histogram`
- Labels: `connection_id`, `subject`, `msg_type`, `status`
- Meaning: NATS 요청 수신부터 NATS reply 발행 완료까지의 전체 처리 시간입니다.

**Outbound Operational Notes**
- 소통률: `rate(outbound_responses_total{status="success"}[5m]) / rate(outbound_requests_total[5m])`
- `status="timeout"` 비율이 높으면 TCP peer 응답 지연 또는 연결 상태(`connection_state`)를 먼저 확인하세요.
- `status="dropped"` 는 NATS subject 라우팅 설정 문제를 의심하세요.
- `end_to_end_duration` 이 증가하면 TCP retry/priority failover 발생 여부를 로그에서 확인하세요.
