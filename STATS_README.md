# TCP Bridge Metrics

TCP Bridge에서 사용하는 Prometheus 메트릭을 정리한 문서입니다.

## Sections

- Connection Metrics
- Connection Operational Notes
- Inbound Traffic Quality Metrics
- Inbound Operational Notes
- Outbound Traffic Quality Metrics
- Outbound Operational Notes

## Connection Metrics

### `tcp_bridge_connection_state`
- Type: `gauge`
- Labels: `connection_id`, `endpoint`
- Meaning: 각 TCP 연결의 현재 상태입니다.
- Values:
  - `0`: disconnected
  - `1`: connecting
  - `2`: ready

### `tcp_bridge_selected_connection`
- Type: `gauge`
- Labels: `connection_id`
- Meaning: 현재 primary/master 로 선택된 connection 여부입니다.
- Values:
  - `1`: selected
  - `0`: not selected

### `tcp_bridge_connection_attempts_total`
- Type: `counter`
- Labels: `connection_id`
- Meaning: dial 시도를 시작한 총 횟수입니다.

### `tcp_bridge_connection_success_total`
- Type: `counter`
- Labels: `connection_id`
- Meaning: handshake 완료 후 `READY` 상태에 진입한 총 횟수입니다.

### `tcp_bridge_connection_failures_total`
- Type: `counter`
- Labels: `connection_id`, `reason`
- Meaning: 연결 시도 또는 handshake 과정에서 실패한 총 횟수입니다.
- Common `reason` values:
  - `dial_timeout`
  - `dial_error`
  - `handshake_timeout`
  - `handshake_error`

### `tcp_bridge_disconnects_total`
- Type: `counter`
- Labels: `connection_id`, `reason`
- Meaning: 연결이 끊어진 총 횟수입니다.
- Common `reason` values:
  - `remote_close`
  - `read_timeout`
  - `read_error`
  - `write_error`
  - `ping_timeout`

### `tcp_bridge_ping_rtt_seconds`
- Type: `histogram`
- Labels: `connection_id`
- Meaning: keep-alive PING 전송 후 PONG 응답까지의 round-trip time 입니다.

### `tcp_bridge_last_activity_timestamp_seconds`
- Type: `gauge`
- Labels: `connection_id`
- Meaning: 마지막 송수신 활동 시각의 Unix timestamp(seconds) 입니다.
- Notes:
  - business frame 송수신 시 갱신됩니다
  - stale connection 탐지에 사용할 수 있습니다

### `tcp_bridge_failover_total`
- Type: `counter`
- Labels: `from`, `to`, `reason`
- Meaning: selected connection 변경 총 횟수입니다.
- Common `reason` values:
  - `selected`
  - `failover`
  - `higher_priority_restored`
  - `connection_lost`
  - `active_changed`

## Connection Operational Notes

- `connection_state` 와 `selected_connection` 을 같이 보면 현재 선택된 primary/master 회선과 standby 회선을 구분할 수 있습니다.
- `connection_attempts_total - connection_success_total` 차이가 계속 커지면 연결 자체가 불안정한 상태일 가능성이 큽니다.
- `disconnects_total{reason="ping_timeout"}` 이 증가하면 peer 응답 정지 또는 네트워크 단절 가능성을 먼저 확인하는 편이 좋습니다.
- `failover_total` 이 짧은 시간에 반복 증가하면 active 회선 flap 상황일 수 있습니다.

## Inbound Traffic Quality Metrics

### `tcp_bridge_inbound_requests_total`
- Type: `counter`
- Labels: `msg_type`
- Meaning: TCP에서 수신한 inbound business request 총 개수입니다.

### `tcp_bridge_inbound_responses_total`
- Type: `counter`
- Labels: `msg_type`, `status`
- Meaning: inbound 요청의 최종 처리 결과 총 개수입니다.
- Common `status` values:
  - `success`
  - `error`
  - `timeout`
  - `dropped`

### `tcp_bridge_inbound_inflight_requests`
- Type: `gauge`
- Labels: `msg_type`
- Meaning: 현재 처리 중인 inbound 요청 수입니다.

### `tcp_bridge_inbound_end_to_end_duration_seconds`
- Type: `histogram`
- Labels: `msg_type`, `status`
- Meaning: TCP inbound 요청 수신부터 최종 TCP 응답 처리까지의 end-to-end 시간입니다.

### `tcp_bridge_inbound_nats_request_duration_seconds`
- Type: `histogram`
- Labels: `msg_type`, `status`
- Meaning: inbound 요청이 NATS request/reply 구간에서 소비한 시간입니다.

### `tcp_bridge_inbound_response_write_duration_seconds`
- Type: `histogram`
- Labels: `msg_type`, `status`
- Meaning: 원래 TCP connection 으로 응답을 쓰는 데 걸린 시간입니다.

### `tcp_bridge_inbound_errors_total`
- Type: `counter`
- Labels: `msg_type`, `error_type`
- Meaning: inbound 처리 중 발생한 에러 총 개수입니다.
- Common `error_type` values:
  - `nats_timeout`
  - `nats_error`
  - `invalid_message_type`
  - `tcp_write_error`
  - `connection_not_ready`
  - `inflight_expired`

### `tcp_bridge_inbound_timeout_total`
- Type: `counter`
- Labels: `msg_type`, `stage`
- Meaning: inbound 처리 중 timeout 발생 횟수입니다.
- Common `stage` values:
  - `nats_wait`
  - `overall`

### `tcp_bridge_inbound_unmatched_response_total`
- Type: `counter`
- Labels: `msg_type`
- Meaning: inflight 만료 등으로 정상 completion 과 매칭되지 못한 inbound 요청 수입니다.

### `tcp_bridge_inbound_worker_tasks_total`
- Type: `counter`
- Labels: `status`
- Meaning: inbound worker 가 완료한 작업 수입니다.
- Common `status` values:
  - `success`
  - `error`
  - `timeout`
  - `dropped`

### `tcp_bridge_inbound_queue_wait_duration_seconds`
- Type: `histogram`
- Meaning: inbound 요청이 worker queue 에서 대기한 시간입니다.

## Inbound Operational Notes

- `inbound_requests_total` 대비 `inbound_responses_total{status="success"}` 비율이 떨어지면 처리 품질 저하를 의심할 수 있습니다.
- `inbound_nats_request_duration_seconds` 가 증가하는데 `response_write_duration_seconds` 는 안정적이면 병목은 NATS 또는 downstream 서비스 쪽일 가능성이 큽니다.
- `inbound_queue_wait_duration_seconds` 와 `inbound_inflight_requests` 가 함께 증가하면 worker 수나 queue size 조정이 필요할 수 있습니다.
- `inbound_timeout_total{stage="nats_wait"}` 이 증가하면 NATS request timeout 설정과 downstream 응답 시간을 먼저 확인하는 편이 좋습니다.

## Outbound Traffic Quality Metrics

### `tcp_bridge_outbound_requests_total`
- Type: `counter`
- Labels: `subject`, `msg_type`
- Meaning: NATS에서 수신한 outbound request 총 개수입니다.

### `tcp_bridge_outbound_responses_total`
- Type: `counter`
- Labels: `subject`, `msg_type`, `status`
- Meaning: outbound 요청의 최종 처리 결과 총 개수입니다.
- Common `status` values:
  - `success`
  - `error`
  - `timeout`
  - `dropped`

### `tcp_bridge_outbound_inflight_requests`
- Type: `gauge`
- Labels: `subject`, `msg_type`
- Meaning: 현재 처리 중인 outbound 요청 수입니다.

### `tcp_bridge_outbound_end_to_end_duration_seconds`
- Type: `histogram`
- Labels: `subject`, `msg_type`, `status`
- Meaning: NATS 요청 수신부터 최종 NATS reply 발행까지의 end-to-end 시간입니다.

### `tcp_bridge_outbound_tcp_response_wait_duration_seconds`
- Type: `histogram`
- Labels: `subject`, `msg_type`, `status`
- Meaning: TCP 요청 전송 후 TCP 응답을 기다리는 시간입니다.

### `tcp_bridge_outbound_nats_reply_duration_seconds`
- Type: `histogram`
- Labels: `subject`, `msg_type`, `status`
- Meaning: 최종 NATS reply 를 발행하는 데 걸린 시간입니다.

### `tcp_bridge_outbound_errors_total`
- Type: `counter`
- Labels: `subject`, `msg_type`, `error_type`
- Meaning: outbound 처리 중 발생한 에러 총 개수입니다.
- Common `error_type` values:
  - `unknown_subject`
  - `serialize_error`
  - `no_ready_connection`
  - `tcp_write_error`
  - `incomplete_write`
  - `tcp_response_timeout`
  - `reply_publish_error`
  - `response_type_mismatch`
  - `connection_mismatch`

### `tcp_bridge_outbound_timeout_total`
- Type: `counter`
- Labels: `subject`, `msg_type`, `stage`
- Meaning: outbound 처리 중 timeout 발생 횟수입니다.
- Common `stage` values:
  - `tcp_wait`
  - `overall`

### `tcp_bridge_outbound_unmatched_response_total`
- Type: `counter`
- Labels: `msg_type`
- Meaning: inflight entry 와 정상 매칭되지 못한 outbound TCP 응답 수입니다.

### `tcp_bridge_outbound_worker_tasks_total`
- Type: `counter`
- Labels: `status`
- Meaning: outbound worker 가 완료한 작업 수입니다.
- Common `status` values:
  - `success`
  - `error`
  - `timeout`
  - `dropped`

### `tcp_bridge_outbound_queue_wait_duration_seconds`
- Type: `histogram`
- Meaning: outbound 요청이 worker queue 에서 대기한 시간입니다.

## Outbound Operational Notes

- `outbound_requests_total` 대비 `outbound_responses_total{status="success"}` 비율이 떨어지면 TCP downstream 또는 reply publish 구간 품질 저하를 의심할 수 있습니다.
- `outbound_tcp_response_wait_duration_seconds` 가 증가하면 TCP peer 응답 지연 또는 failover/retry 증가를 먼저 확인하는 편이 좋습니다.
- `outbound_queue_wait_duration_seconds` 와 `outbound_inflight_requests` 가 함께 증가하면 outbound worker 수나 queue size 조정이 필요할 수 있습니다.
- `outbound_timeout_total{stage="tcp_wait"}` 이 증가하면 TCP response timeout 설정과 peer 응답 시간을 먼저 확인하는 편이 좋습니다.
