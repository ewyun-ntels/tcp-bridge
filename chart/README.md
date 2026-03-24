# TCP Bridge Helm Chart

TCP Bridge는 TCP 서버와 NATS 메시징 시스템 간의 브리지 역할을 하는 애플리케이션입니다.

## 설치 방법

### Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- NATS 서버 (선택적으로 차트의 dependency로 설치 가능)
- Prometheus Operator (ServiceMonitor 사용 시)

### 기본 설치

```bash
# Helm chart 설치 (현재 디렉토리에서)
helm install tcp-bridge ./chart -n upm-messaging --create-namespace

# 다른 네임스페이스에 설치
helm install tcp-bridge ./chart -n my-namespace --create-namespace
```

### Custom Values로 설치

```bash
# values.yaml을 복사하여 수정
cp chart/values.yaml my-values.yaml

# 수정한 values 파일로 설치
helm install tcp-bridge ./chart -f my-values.yaml -n upm-messaging
```

### 주요 설정 옵션

#### 1. 이미지 설정

```yaml
image:
  repository: your-registry/tcp-bridge
  tag: "1.0.0"
  pullPolicy: IfNotPresent
```

#### 2. TCP 엔드포인트 설정

```yaml
config:
  tcp:
    endpoints:
      - host: "tcp-server-1.example.com"
        port: 8000
        priority: 0  # Primary
      - host: "tcp-server-2.example.com"
        port: 8000
        priority: 1  # Secondary
```

#### 3. NATS 연결 설정

```yaml
config:
  nats:
    urls:
      - "nats://nats.upm-messaging.svc.cluster.local:4222"
```

#### 4. ServiceMonitor 활성화

**Prometheus Operator가 설치되어 있어야 합니다.**

```yaml
serviceMonitor:
  enabled: true
  labels:
    release: kube-prometheus-stack
  interval: 10s
  namespace: ""
```

ServiceMonitor가 생성되면 Prometheus Operator가 자동으로 각 Pod의 메트릭을 수집합니다.

#### 5. 리소스 제한

```yaml
resources:
  limits:
    cpu: 500m
    memory: 512Mi
  requests:
    cpu: 100m
    memory: 128Mi
```

#### 6. Multus + macvlan + whereabouts

```yaml
replicaCount: 2

multus:
  enabled: true
  createNAD: true
  network:
    name: tcp-bridge-net
    master: eth0
    ipam:
      range: "10.10.10.20/29"
      rangeStart: "10.10.10.21"
      rangeEnd: "10.10.10.22"
      gateway: "10.10.10.1"
```

이 설정은 두 개의 Pod가 동일한 NAD를 사용하고, `whereabouts` 풀에서 두 개 IP를 나눠 받는 구조입니다.

## 업그레이드

```bash
# Chart 업그레이드
helm upgrade tcp-bridge ./chart -n upm-messaging

# Custom values로 업그레이드
helm upgrade tcp-bridge ./chart -f my-values.yaml -n upm-messaging
```

## 삭제

```bash
helm uninstall tcp-bridge -n upm-messaging
```

## 설치 확인

```bash
# Pod 상태 확인
kubectl get pods -n upm-messaging -l app.kubernetes.io/name=tcp-bridge

# 로그 확인
kubectl logs -n upm-messaging -l app.kubernetes.io/name=tcp-bridge -f

# Service 확인
kubectl get svc -n upm-messaging -l app.kubernetes.io/name=tcp-bridge

# ServiceMonitor 확인 (활성화된 경우)
kubectl get servicemonitor -n upm-monitoring tcp-bridge
```

## 메트릭 확인

### Port-forward로 메트릭 확인

```bash
kubectl port-forward -n upm-messaging svc/tcp-bridge 8080:8080
curl http://localhost:8080/metrics
```

### Prometheus에서 확인

ServiceMonitor가 활성화되어 있으면 Prometheus에서 자동으로 수집됩니다.

```promql
# Pod별 메트릭 확인
tcp_bridge_connection_state{pod="tcp-bridge-xxxxx"}

# 전체 요청 수
sum(rate(tcp_bridge_outbound_requests_total[5m]))
```

## 설정 가이드

### 개발 환경 예시

```yaml
replicaCount: 1

config:
  server:
    environment: "development"
  logging:
    level: "debug"
    format: "text"

resources:
  requests:
    cpu: 50m
    memory: 64Mi

serviceMonitor:
  enabled: false
```

설치:
```bash
helm install tcp-bridge ./chart -f dev-values.yaml -n dev
```

### 운영 환경 예시

```yaml
replicaCount: 2

config:
  server:
    environment: "production"
  logging:
    level: "info"
    format: "json"

resources:
  limits:
    cpu: 1000m
    memory: 1Gi
  requests:
    cpu: 200m
    memory: 256Mi

serviceMonitor:
  enabled: true
  labels:
    release: kube-prometheus-stack
  namespace: upm-monitoring

affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchExpressions:
          - key: app.kubernetes.io/name
            operator: In
            values:
            - tcp-bridge
        topologyKey: kubernetes.io/hostname
```

설치:
```bash
helm install tcp-bridge ./chart -f prod-values.yaml -n production
```

`prod-values.yaml`은 `chart/values.yaml`을 복사해서 필요한 값만 덮어쓰는 방식으로 사용하면 됩니다.

## Troubleshooting

### Pod가 시작되지 않을 때

```bash
# Pod 상태 확인
kubectl describe pod -n upm-messaging -l app.kubernetes.io/name=tcp-bridge

# 이벤트 확인
kubectl get events -n upm-messaging --sort-by='.lastTimestamp'
```

### ConfigMap 확인

```bash
kubectl get configmap -n upm-messaging tcp-bridge-config -o yaml
```

### ServiceMonitor가 작동하지 않을 때

```bash
# ServiceMonitor 존재 확인
kubectl get servicemonitor -n upm-monitoring

# Prometheus targets 확인
# Prometheus UI > Status > Targets에서 tcp-bridge 확인
kubectl port-forward -n upm-monitoring svc/kube-prometheus-stack-prometheus 9090:9090
# 브라우저에서 http://localhost:9090/targets 접속
```

## Values 파일 구조

전체 설정 옵션은 `values.yaml` 파일을 참조하세요.

주요 섹션:
- `image`: 컨테이너 이미지 설정
- `replicaCount`: Pod 복제 수
- `service`: Kubernetes Service 설정
- `serviceMonitor`: Prometheus ServiceMonitor 설정
- `config`: TCP Bridge 애플리케이션 설정
  - `server`: 서버 기본 설정
  - `tcp`: TCP 엔드포인트 및 연결 설정
  - `nats`: NATS 연결 및 라우팅 설정
  - `message_handler`: 메시지 처리 워커 설정
  - `metrics`: Prometheus 메트릭 설정
  - `logging`: 로그 설정
- `resources`: CPU/메모리 리소스

## 참고

- [Helm Documentation](https://helm.sh/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator)
