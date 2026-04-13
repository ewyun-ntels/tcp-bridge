# TCP Bridge Stats Guide

`tcp-bridge`가 현재 노출하는 Prometheus raw metric, 이를 기반으로 한 recording rule, 그리고 실제 alert rule 해석을 한 번에 정리한 문서다.

기준 소스:

- Metric 구현: `internal/metrics/metrics.go`
- Metric 발생 지점: `internal/worker/bridge_handler.go`, `internal/worker/inbound_pool.go`, `internal/worker/outbound_pool.go`
- Recording rule: `monitoring/tcp-bridge-recording-rule.yaml`
- Alert rule: `monitoring/tcp-bridge-prometheus-rule.yaml`

## 1. 현재 운영 관점 요약

현재 구현은 크게 3개 층으로 나뉜다.

1. Raw metric
   TCP connection 상태, inbound/outbound 요청 수, 결과 수, end-to-end duration을 직접 수집한다.
2. Recording rule
   2분 window 기준으로 소통률, 에러율, 에러 건수를 pod 단위로 집계한다.
3. Alert rule
   recording rule 결과값을 기준으로 정상/이상 알림을 발생시킨다.

현재 alert는 사실상 `inbound/outbound 품질 감시`에 집중되어 있다.

- `connection_state` 관련 alert는 정의돼 있지만 주석 처리되어 있어 실제 활성 감시는 아님
- `selected_connection`도 raw metric으로는 존재하지만 현재 recording/alert에서 사용하지 않음
- histogram metric도 수집되지만 현재 alert 판단에는 사용하지 않음

## 2. 라벨 구조와 해석 시 주의점

Raw metric 라벨은 connection 중심이고, recording/alert는 pod 중심으로 재집계된다.

- `connection_id`
  개별 TCP connection 식별자
- `endpoint`
  해당 connection이 연결하는 대상 endpoint
- `msg_type`
  프레임 타입 문자열
- `subject`
  outbound NATS subject
- `status`
  `success`, `error`, `timeout`, `dropped`

Recording rule에서는 `sum by (job, namespace, pod)` 또는 `max by (job, namespace, pod, connection_id)`로 집계한 뒤, 아래 정규식으로 `sys_id`를 pod 이름에서 추출한다.

```promql
label_replace(..., "sys_id", "$1", "pod", ".*(tcp-bridge-[0-9]+).*")
```

즉 현재 rule에서의 `sys_id`는 업무 시스템 ID가 아니라 사실상 `tcp-bridge-0`, `tcp-bridge-1` 같은 pod 식별자다.

또한 outbound metric은 요청 초기에 아직 connection이 선택되지 않은 경우가 있어 일부 시계열이 `connection_id="unknown"`으로 기록될 수 있다.

## 3. Raw Metric 정의

### 3.1 Connection Metrics

#### `tcp_bridge_connection_state`

- Type: `gauge`
- Labels: `connection_id`, `endpoint`
- 의미: connection 상태
- 값:
  - `0`: disconnected
  - `1`: connecting
  - `2`: ready

생성 위치:

- `Metrics.SetConnectionState()`

운영 해석:

- `2`면 해당 connection이 실제 송수신 가능한 READY 상태
- `1`이 오래 유지되면 재연결 루프 또는 HELLO/handshake 구간 정체 가능성
- 현재 alert에는 연결되어 있지 않지만, 장애 분석 시 가장 먼저 봐야 하는 기본 지표

#### `tcp_bridge_selected_connection`

- Type: `gauge`
- Labels: `connection_id`
- 의미: 현재 primary/master로 선택된 connection 여부
- 값:
  - `1`: selected
  - `0`: not selected

생성 위치:

- `Metrics.SetActiveConnection()`

운영 해석:

- outbound 전송 시 어떤 회선이 active인지 확인할 수 있다
- 현재 recording/alert에서는 사용하지 않는다

### 3.2 Inbound Traffic Metrics

Flow:

`TCP Server -> tcp-bridge -> NATS -> tcp-bridge -> TCP Server`

#### `tcp_bridge_inbound_requests_total`

- Type: `counter`
- Labels: `connection_id`, `msg_type`
- 의미: TCP에서 수신한 inbound request 총량

증가 조건:

- TCP request frame이 `HandleInboundFrame()`에 들어오면 enqueue 성공 여부와 무관하게 증가

#### `tcp_bridge_inbound_responses_total`

- Type: `counter`
- Labels: `connection_id`, `msg_type`, `status`
- 의미: inbound 요청의 최종 처리 결과 총량

`status` 의미:

- `success`: NATS 응답을 받아 TCP로 정상 응답
- `error`: NATS request 실패 또는 TCP 응답 전송 실패
- `timeout`: inflight timeout 또는 NATS timeout
- `dropped`: inbound worker queue 포화 또는 미정의 msg_type

증가 조건:

- queue full 시 `dropped`
- subject 매핑 실패 시 `dropped`
- NATS timeout 시 `timeout`
- 기타 NATS/TCP 처리 실패 시 `error`
- TCP 응답 성공 시 `success`

주의:

- timeout은 worker 내부 timeout뿐 아니라 inflight expiry 핸들러에서도 증가할 수 있다

#### `tcp_bridge_inbound_end_to_end_duration_seconds`

- Type: `histogram`
- Labels: `connection_id`, `msg_type`, `status`
- Buckets: `prometheus.DefBuckets`
- 의미: TCP 요청 수신부터 최종 TCP 응답 처리까지 전체 시간

특징:

- `frame.ReceivedAt`가 존재할 때만 기록
- 성공/실패/timeout/dropped를 모두 같은 histogram에 `status` 라벨로 구분해 적재

### 3.3 Outbound Traffic Metrics

Flow:

`NATS -> tcp-bridge -> TCP Server -> tcp-bridge -> NATS`

#### `tcp_bridge_outbound_requests_total`

- Type: `counter`
- Labels: `connection_id`, `subject`, `msg_type`
- 의미: NATS에서 수신한 outbound request 총량

증가 조건:

- subject 해석 실패 시에도 1 증가
- 정상 route 해석 후 실제 TCP 전송 시도 전에도 1 증가

주의:

- connection 선택 전에 증가하므로 초기 라벨값이 `connection_id="unknown"`일 수 있다

#### `tcp_bridge_outbound_responses_total`

- Type: `counter`
- Labels: `connection_id`, `subject`, `msg_type`, `status`
- 의미: outbound 요청의 최종 처리 결과 총량

`status` 의미:

- `success`: TCP 응답을 받아 NATS reply 발행 성공
- `error`: 직렬화 실패, write 실패 누적, reply 발행 실패, ready connection 부재 등 일반 실패
- `timeout`: 모든 retry/priority 경로에서 응답 timeout
- `dropped`: 알 수 없는 NATS subject 또는 queue 포화

주의:

- `no ready connection` 같은 상태도 별도 status로 분리되지 않고 현재는 `error`로 집계된다
- 코드에 `classifyOutboundWriteError()`가 있지만 현재 metric status 분기에는 연결되지 않는다

#### `tcp_bridge_outbound_end_to_end_duration_seconds`

- Type: `histogram`
- Labels: `connection_id`, `subject`, `msg_type`, `status`
- Buckets: `prometheus.DefBuckets`
- 의미: NATS 요청 수신부터 NATS reply 발행까지 전체 시간

특징:

- 요청 수신 직후부터 측정하므로 queue 대기, retry, priority failover, TCP 응답 대기 시간이 모두 포함된다

## 4. Recording Rule 정리

모든 recording rule interval은 `30s`다.

### 4.1 Connectivity

#### `tcp_bridge:ready_connection_state:max`

```promql
label_replace(
  max by (job, namespace, pod, connection_id) (tcp_bridge_connection_state),
  "sys_id", "$1", "pod", ".*(tcp-bridge-[0-9]+).*"
)
```

의미:

- connection별 현재 상태의 최대값
- 사실상 `connection_state` raw metric을 pod 문맥으로 보기 좋게 감싼 형태

한계:

- 현재 alert rule에서는 연결성 관련 항목이 주석 처리되어 실사용되지 않음

### 4.2 Inbound

#### `tcp_bridge:inbound_communication_rate:2m`

```promql
sum(rate(tcp_bridge_inbound_responses_total{status="success"}[2m]))
/
clamp_min(sum(rate(tcp_bridge_inbound_requests_total[2m])), 1)
```

실제 rule은 `job, namespace, pod` 단위 집계 후 `sys_id`를 붙인다.

의미:

- 2분 동안 요청 대비 성공 응답 비율
- 정상에 가까울수록 `1.0`

해석:

- request는 증가했지만 아직 response가 늦게 닫히는 구간에서는 일시적으로 하락할 수 있다
- timeout/error/dropped가 늘면 감소한다

#### `tcp_bridge:inbound_error_rate:2m`

```promql
sum(rate(tcp_bridge_inbound_responses_total{status=~"error|timeout|dropped"}[2m]))
/
clamp_min(sum(rate(tcp_bridge_inbound_responses_total[2m])), 1)
```

의미:

- 2분 동안 전체 응답 중 실패 응답 비율

주의:

- 분모가 `requests_total`이 아니라 `responses_total`
- 따라서 "요청 대비 실패율"이 아니라 "완결된 응답 중 실패 비율"이다

#### `tcp_bridge:inbound_error_count:2m`

- 2분 동안 `error|timeout|dropped` 증가 건수
- 주석상 레거시 호환용
- 현재 alert rule에서는 직접 사용하지 않음

### 4.3 Outbound

#### `tcp_bridge:outbound_communication_rate:2m`

구조는 inbound와 동일하며 대상 metric만 outbound로 바뀐다.

의미:

- 2분 동안 outbound request 대비 success 응답 비율

주의:

- outbound request는 `connection_id="unknown"` 시계열을 포함할 수 있다
- recording rule은 pod 단위 합산이라 connection_id 차이는 최종 비율 계산에서 사라진다

#### `tcp_bridge:outbound_error_rate:2m`

의미:

- 2분 동안 outbound 응답 중 `error|timeout|dropped` 비율

특징:

- TCP write 실패, ready connection 부재, reply publish 실패, timeout이 모두 이 수치에 반영된다

#### `tcp_bridge:outbound_error_count:2m`

- 2분 동안 `error|timeout|dropped` 증가 건수
- 현재 alert rule에서는 직접 사용하지 않음

## 5. Alert Rule 정리

모든 활성 alert는 `for: 1m` 조건을 가진다. 즉 2분 recording rule 값이 1분 연속 조건을 만족할 때 firing된다.

### 5.1 현재 활성화된 alert

#### Inbound communication

- `TcpBridgeInboundCommunicationOk`
  - 조건: `tcp_bridge:inbound_communication_rate:2m >= 0.95`
  - severity: `normal`
  - errCode: `3100`
- `TcpBridgeInboundCommunicationFailure`
  - 조건: `tcp_bridge:inbound_communication_rate:2m < 0.95`
  - severity: `critical`
  - errCode: `3101`

해석:

- 최근 2분 동안 inbound request 대비 success 응답이 95% 미만이면 critical
- `timeout`, `error`, `dropped`가 조금만 늘어도 바로 반영된다

#### Inbound errors

- `TcpBridgeInboundNoError`
  - 조건: `< 1%`
  - severity: `normal`
  - errCode: `3200`
- `TcpBridgeInboundLowErrorRate`
  - 조건: `>= 1% and < 3%`
  - severity: `minor`
  - errCode: `3201`
- `TcpBridgeInboundModerateErrorRate`
  - 조건: `>= 3% and < 5%`
  - severity: `major`
  - errCode: `3202`
- `TcpBridgeInboundHighErrorRate`
  - 조건: `>= 5%`
  - severity: `critical`
  - errCode: `3203`

해석:

- inbound는 NATS request/response 경로 문제, worker queue 포화, TCP 응답 전송 실패가 주 원인이다

#### Outbound communication

- `TcpBridgeOutboundCommunicationOk`
  - 조건: `tcp_bridge:outbound_communication_rate:2m >= 0.95`
  - severity: `normal`
  - errCode: `4100`
- `TcpBridgeOutboundCommunicationFailure`
  - 조건: `tcp_bridge:outbound_communication_rate:2m < 0.95`
  - severity: `critical`
  - errCode: `4101`

해석:

- 최근 2분 동안 outbound request 대비 success 응답이 95% 미만이면 critical
- TCP peer 지연, connection failover, reply publish 실패가 직접 반영된다

#### Outbound errors

- `TcpBridgeOutboundNoError`
  - 조건: `< 1%`
  - severity: `normal`
  - errCode: `4200`
- `TcpBridgeOutboundLowErrorRate`
  - 조건: `>= 1% and < 3%`
  - severity: `minor`
  - errCode: `4201`
- `TcpBridgeOutboundModerateErrorRate`
  - 조건: `>= 3% and < 5%`
  - severity: `major`
  - errCode: `4202`
- `TcpBridgeOutboundHighErrorRate`
  - 조건: `>= 5%`
  - severity: `critical`
  - errCode: `4203`

해석:

- outbound는 connection ready 부재, TCP write 실패, TCP 응답 timeout, NATS reply 발행 실패가 핵심 원인이다

### 5.2 현재 비활성화된 alert

아래는 rule 파일에는 있으나 주석 처리되어 실제 비활성 상태다.

- `TcpBridgeMetricsUnavailable`
  - 의미: metrics scrape 불가 또는 `up{job="tcp-bridge"} == 0`
- `TcpBridgeConnectionReady`
  - 의미: `ready_connection_state:max >= 2`
- `TcpBridgeConnectionNotReady`
  - 의미: `ready_connection_state:max < 2`

즉 현재 설정만 보면 "메트릭 수집 자체 장애"와 "회선 준비 상태"는 알림에서 빠져 있다.

## 6. 상태값별 원인 매핑

### `success`

- inbound: NATS 응답 수신 후 TCP 응답 전송 성공
- outbound: TCP 응답 수신 후 NATS reply 발행 성공

### `error`

- inbound:
  - NATS request 실패
  - TCP 응답 전송 실패
- outbound:
  - frame serialize 실패
  - TCP write 실패 누적 후 최종 실패
  - ready connection 없음
  - NATS reply publish 실패

### `timeout`

- inbound:
  - NATS request timeout
  - inflight timeout expiry
- outbound:
  - 모든 retry/priority 경로에서 응답 timeout

### `dropped`

- inbound:
  - worker queue full
  - 미정의 msg_type
- outbound:
  - outbound queue full
  - 미정의 NATS subject

## 7. 운영 시 바로 보는 순서

### inbound 알림이 떴을 때

1. `tcp_bridge:inbound_communication_rate:2m`
2. `tcp_bridge:inbound_error_rate:2m`
3. `tcp_bridge_inbound_responses_total{status=...}`
4. `tcp_bridge_inbound_end_to_end_duration_seconds`
5. NATS downstream 상태와 inbound worker queue 포화 여부

우선 원인 분기:

- `timeout` 위주면 NATS/downstream 지연
- `dropped` 위주면 queue 설정 또는 routing 누락
- `error` 위주면 NATS 요청 실패나 TCP 응답 write 실패

### outbound 알림이 떴을 때

1. `tcp_bridge:outbound_communication_rate:2m`
2. `tcp_bridge:outbound_error_rate:2m`
3. `tcp_bridge_outbound_responses_total{status=...}`
4. `tcp_bridge_connection_state`
5. `tcp_bridge_selected_connection`
6. `tcp_bridge_outbound_end_to_end_duration_seconds`

우선 원인 분기:

- `timeout` 위주면 TCP peer 응답 지연 또는 failover 반복
- `error` 위주면 connection ready 부재, write 실패, reply publish 실패
- `dropped` 위주면 subject routing 또는 queue 포화

## 8. 현재 구현 기준 갭과 판단

현재 메트릭/룰 조합을 기준으로 보면 다음 특성이 있다.

- 장점
  - inbound/outbound 품질을 요청 성공률과 에러율로 단순하게 볼 수 있다
  - status 라벨이 명확해 원인 분기 초동은 빠르다
  - histogram이 있어 나중에 latency SLO 확장이 가능하다

- 한계
  - 연결 상태 alert가 비활성이라 품질 저하 전에 선제 감지가 어렵다
  - `selected_connection`은 수집되지만 운영 rule에서 활용되지 않는다
  - error rate 분모가 `responses_total`이라 미응답 요청 적체는 직접 드러나지 않는다
  - outbound `error` 안에 여러 실패 원인이 섞여 있어 세부 원인 분리가 어렵다
  - histogram 기반 p95/p99 recording/alert가 없어 지연 악화는 현재 직접 감시하지 못한다
  - `sys_id`라는 이름이 실제로는 pod 식별자라 운영자 해석을 혼동시킬 수 있다

## 9. 결론

현재 `tcp-bridge`의 통계 체계는 "connection 세부 상태를 풍부하게 보는 체계"라기보다 "inbound/outbound 요청 품질을 2분 단위로 감시하는 체계"에 가깝다.

실제로 alert에 반영되는 핵심 지표는 아래 4개다.

- `tcp_bridge:inbound_communication_rate:2m`
- `tcp_bridge:inbound_error_rate:2m`
- `tcp_bridge:outbound_communication_rate:2m`
- `tcp_bridge:outbound_error_rate:2m`

따라서 운영 중 알림 해석도 아래처럼 가져가는 것이 맞다.

- inbound 이상: NATS/downstream 처리 품질 문제로 본다
- outbound 이상: TCP peer 연결/응답 품질 문제로 본다
- connection_state와 selected_connection은 원인 분석용 보조 지표로 본다

연결 자체의 가용성까지 적극 감시하려면, 현재 주석 처리된 connectivity alert를 활성화하거나 별도 rule을 추가하는 보완이 필요하다.
