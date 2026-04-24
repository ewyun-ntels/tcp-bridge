# TCP Bridge Stats Guide

이 문서는 `tcp-bridge`가 노출하는 운영 메트릭을 `request`, `outcome`, `response_sent` 기준으로 정리한 문서다.

기준 소스:

- Metric 구현: `internal/metrics/metrics.go`
- Metric 발생 지점: `internal/worker/bridge_handler.go`, `internal/worker/inbound_pool.go`, `internal/worker/outbound_pool.go`
- Recording rule: `chart/templates/prometheusrule-recording.yaml`, `monitoring/tcp-bridge-recording-rule.yaml`
- Alert rule: `chart/templates/prometheusrule-alerts.yaml`, `monitoring/tcp-bridge-prometheus-rule.yaml`

## 1. 설계 원칙

현재 메트릭은 세 층으로 나뉜다.

1. `*_requests_total`
   요청이 처리 경로에 진입했을 때 증가한다.
2. `*_outcomes_total`
   요청의 최종 처리 결과를 요청당 정확히 1번 기록한다.
3. `*_responses_sent_total`
   실제 외부로 응답을 성공적으로 송신했을 때만 증가한다.

이 구조로 해석하면:

- `requests_total` vs `outcomes_total`
  내부 처리 completeness 확인용
- `requests_total` vs `responses_sent_total`
  외부 응답 송신 completeness 확인용

즉 `requests_total`과 직접 매칭해야 하는 대상은 `responses_total`이 아니라 `outcomes_total`이다.

## 2. 공통 해석

### 2.1 Status

`*_outcomes_total`의 `status`는 아래 4가지다.

- `success`
  요청이 정상 흐름으로 완료됨
- `error`
  일반 실패로 종료됨
- `timeout`
  timeout으로 종료됨
- `dropped`
  처리 전에 폐기되었거나 즉시 거절됨

### 2.2 Response Result

`*_responses_sent_total`의 `result`는 응답 payload 성격을 뜻한다.

- `success`
  정상 응답 payload 송신 성공
- `error`
  에러 응답 payload 송신 성공

중요:

- `responses_sent_total`은 송신 성공시에만 증가한다
- 송신을 시도했지만 실패했다면 `responses_sent_total`은 증가하지 않는다
- 이런 경우 최종 결과는 `outcomes_total`에서 확인해야 한다

### 2.3 `responses_sent_total`이 증가하지 않는 경우

현재 구현 기준으로 `*_responses_sent_total`이 증가하지 않는 경우는 두 부류다.

1. 응답 송신을 아예 하지 않는 분기
2. 응답 송신을 시도했지만 실제 송신에 실패한 분기

Inbound에서 증가하지 않는 대표 경우:

- `unknown_message_type`
  라우팅 대상이 없어 `outcomes_total{status="dropped",reason="unknown_message_type"}`만 기록하고 종료한다
- `inflight_expired`
  inflight cleanup callback에서 `outcomes_total{status="timeout",reason="inflight_expired"}`만 기록한다
- `queue_full`
  TCP 에러 응답 송신을 시도하지만 write가 실패하면 `responses_sent_total`은 증가하지 않는다
- `nats_request_timeout`, `nats_request_failed`
  TCP 에러 응답 송신을 시도하지만 write가 실패하면 `responses_sent_total`은 증가하지 않는다
- `tcp_response_write_failed`
  정상 TCP 응답 송신이 실패한 경우이므로 `responses_sent_total`은 증가하지 않는다

Outbound에서 증가하지 않는 대표 경우:

- `missing_reply_subject`
  reply subject가 없어 `outcomes_total{status="dropped",reason="missing_reply_subject"}`만 기록하고 종료한다
- `reply_publish_failed`
  TCP 응답은 받았지만 NATS reply publish가 실패했으므로 `responses_sent_total`은 증가하지 않는다
- `queue_full`, `unknown_subject`
  NATS 에러 reply 송신을 시도하지만 publish가 실패하면 `responses_sent_total`은 증가하지 않는다
- `frame_serialize_failed`, `no_ready_connection`, `tcp_request_write_failed`, `tcp_response_timeout`
  최종 실패 후 NATS 에러 reply 송신을 시도하지만 publish가 실패하면 `responses_sent_total`은 증가하지 않는다

정리:

- 설계상 응답 송신 자체가 없는 분기와
- 응답 송신을 시도했지만 실패한 분기를
  구분해서 봐야 한다
- 요청 처리 완료 여부는 `outcomes_total`
- 외부 응답 송신 성공 여부는 `responses_sent_total`
  로 해석한다

## 3. Connection Metrics

### `tcp_bridge_connection_state`

- Type: `gauge`
- Labels: `connection_id`, `connection_name`, `endpoint`
- 값:
  - `0`: disconnected
  - `1`: connecting
  - `2`: ready

### `tcp_bridge_selected_connection`

- Type: `gauge`
- Labels: `connection_id`, `connection_name`
- 값:
  - `1`: selected
  - `0`: not selected

## 4. Inbound Metrics

Flow:

`TCP Server -> tcp-bridge -> NATS -> tcp-bridge -> TCP Server`

### `tcp_bridge_inbound_requests_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `msg_type`
- 의미: TCP에서 inbound request를 수신한 총량

증가 시점:

- TCP reader가 inbound request frame을 받았을 때 1 증가

### `tcp_bridge_inbound_outcomes_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `msg_type`, `status`, `reason`
- 의미: inbound 요청의 최종 처리 결과 총량

증가 시점:

- 요청당 최종적으로 1번만 증가

`reason`:

- `ok`
  정상 처리 후 TCP 정상 응답 송신까지 완료
- `queue_full`
  inbound worker queue 포화
- `unknown_message_type`
  inbound 라우팅 대상이 없는 `msg_type`
- `nats_request_timeout`
  NATS request timeout
- `nats_request_failed`
  timeout 제외 NATS request 실패
- `tcp_response_write_failed`
  TCP 응답 송신 실패
- `inflight_expired`
  inflight 만료 처리로 timeout 종료

해석 예시:

- 정상 처리:
  `requests +1`, `outcomes{success,ok} +1`, `responses_sent{success} +1`
- queue full 후 에러 응답 송신 성공:
  `requests +1`, `outcomes{dropped,queue_full} +1`, `responses_sent{error} +1`
- unknown message type 드롭:
  `requests +1`, `outcomes{dropped,unknown_message_type} +1`
- NATS timeout 후 에러 응답 송신 성공:
  `requests +1`, `outcomes{timeout,nats_request_timeout} +1`, `responses_sent{error} +1`
- NATS timeout 후 에러 응답 송신 실패:
  `requests +1`, `outcomes{timeout,nats_request_timeout} +1`

### `tcp_bridge_inbound_responses_sent_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `msg_type`, `result`
- 의미: 외부 TCP peer로 응답 송신에 성공한 총량

증가 시점:

- 정상 응답 TCP write 성공 시 `result="success"`
- 에러 응답 TCP write 성공 시 `result="error"`

### `tcp_bridge_inbound_end_to_end_duration_seconds`

- Type: `histogram`
- Labels: `connection_id`, `connection_name`, `msg_type`, `status`
- 의미: inbound 요청 수신부터 최종 종료까지의 전체 시간

## 5. Outbound Metrics

Flow:

`NATS -> tcp-bridge -> TCP Server -> tcp-bridge -> NATS`

### `tcp_bridge_outbound_requests_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `subject`, `msg_type`
- 의미: NATS에서 outbound request를 수신한 총량

증가 시점:

- outbound request의 처리 결과 라벨이 확정되는 시점에 1 증가

주의:

- queue full, unknown subject, missing reply subject처럼 TCP connection 선택 전 종료된 요청은 `connection_id="unknown"`, `connection_name="unknown"`으로 기록될 수 있다

### `tcp_bridge_outbound_outcomes_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `subject`, `msg_type`, `status`, `reason`
- 의미: outbound 요청의 최종 처리 결과 총량

증가 시점:

- 요청당 최종적으로 1번만 증가

`reason`:

- `ok`
  TCP 응답을 받고 NATS reply 송신까지 완료
- `queue_full`
  outbound worker queue 포화
- `missing_reply_subject`
  reply subject 없이 들어와 즉시 거절된 요청
- `unknown_subject`
  outbound 라우팅 대상이 없는 subject
- `frame_serialize_failed`
  TCP frame 직렬화 실패
- `no_ready_connection`
  사용 가능한 READY connection 없음
- `tcp_request_write_failed`
  모든 시도에서 TCP request write 실패
- `tcp_response_timeout`
  모든 retry/priority 경로에서 TCP 응답 timeout
- `reply_publish_failed`
  TCP 응답은 받았지만 NATS reply publish 실패

해석 예시:

- 정상 처리:
  `requests +1`, `outcomes{success,ok} +1`, `responses_sent{success} +1`
- queue full 후 에러 reply 송신 성공:
  `requests +1`, `outcomes{dropped,queue_full} +1`, `responses_sent{error} +1`
- reply subject 없이 즉시 거절:
  `requests +1`, `outcomes{dropped,missing_reply_subject} +1`
- unknown subject 거절 후 에러 reply 송신 성공:
  `requests +1`, `outcomes{dropped,unknown_subject} +1`, `responses_sent{error} +1`
- TCP 응답 timeout 후 에러 reply 송신 성공:
  `requests +1`, `outcomes{timeout,tcp_response_timeout} +1`, `responses_sent{error} +1`
- TCP 응답은 받았지만 NATS reply publish 실패:
  `requests +1`, `outcomes{error,reply_publish_failed} +1`

### `tcp_bridge_outbound_responses_sent_total`

- Type: `counter`
- Labels: `connection_id`, `connection_name`, `subject`, `msg_type`, `result`
- 의미: NATS requester에게 reply 송신에 성공한 총량

증가 시점:

- 정상 reply publish 성공 시 `result="success"`
- 에러 reply publish 성공 시 `result="error"`

### `tcp_bridge_outbound_end_to_end_duration_seconds`

- Type: `histogram`
- Labels: `connection_id`, `connection_name`, `subject`, `msg_type`, `status`
- 의미: outbound 요청 수신부터 최종 종료까지의 전체 시간

## 6. 운영 쿼리

### 6.1 요청 대비 처리 완료율

```promql
sum(increase(tcp_bridge_inbound_outcomes_total[5m]))
/
clamp_min(sum(increase(tcp_bridge_inbound_requests_total[5m])), 1)
```

```promql
sum(increase(tcp_bridge_outbound_outcomes_total[5m]))
/
clamp_min(sum(increase(tcp_bridge_outbound_requests_total[5m])), 1)
```

정상적으로는 장기적으로 `1`에 수렴해야 한다.

### 6.2 요청 대비 실제 응답 송신율

```promql
sum(increase(tcp_bridge_inbound_responses_sent_total[5m]))
/
clamp_min(sum(increase(tcp_bridge_inbound_requests_total[5m])), 1)
```

```promql
sum(increase(tcp_bridge_outbound_responses_sent_total[5m]))
/
clamp_min(sum(increase(tcp_bridge_outbound_requests_total[5m])), 1)
```

이 값은 내부 처리 완료와 외부 응답 송신 성공을 분리해서 볼 때 사용한다.

### 6.3 실패 원인 분포

```promql
sum by (reason) (increase(tcp_bridge_inbound_outcomes_total{status=~"error|timeout|dropped"}[5m]))
```

```promql
sum by (reason) (increase(tcp_bridge_outbound_outcomes_total{status=~"error|timeout|dropped"}[5m]))
```

## 7. Recording Rule

recording rule은 `responses_total`이 아니라 `outcomes_total`을 기준으로 집계한다.

- `tcp_bridge:inbound_outcomes_total:increase2m`
- `tcp_bridge:inbound_error_ratio:2m`
- `tcp_bridge:outbound_outcomes_total:increase2m`
- `tcp_bridge:outbound_error_ratio:2m`

`error_ratio`는 다음 의미다.

- 분자: `status=error|timeout|dropped` 인 outcome 수
- 분모: 전체 outcome 수

즉 “완료된 요청 중 실패 비율”이다.

## 8. Alert Rule

현재 alert도 `outcomes_total` 기준으로 동작한다.

- inbound:
  `tcp_bridge:inbound_error_ratio:2m >= 0.10`
  그리고 `tcp_bridge:inbound_outcomes_total:increase2m >= 5`
- outbound:
  `tcp_bridge:outbound_error_ratio:2m >= 0.10`
  그리고 `tcp_bridge:outbound_outcomes_total:increase2m >= 5`

이 조건은 표본 수가 너무 적은 순간값 노이즈를 줄이기 위한 최소 완료 건수 조건이다.
