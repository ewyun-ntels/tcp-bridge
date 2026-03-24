# TCP Bridge - Helm Chart Quick Start

## 빠른 배포 가이드

### 1. Chart 검증

```bash
# Chart 문법 검증
helm lint chart/

# 생성될 리소스 미리보기 (dry-run)
helm template tcp-bridge chart/ | less

# 커스텀 values 파일로 미리보기
helm template tcp-bridge chart/ -f my-values.yaml | less
```

### 2. 기본 배포 (개발 환경)

```bash
# 기본 values로 배포
helm install tcp-bridge chart/ -n upm-messaging --create-namespace

# 또는 values.yaml을 복사해 수정 후 배포
cp chart/values.yaml my-values.yaml
vi my-values.yaml
helm install tcp-bridge chart/ -f my-values.yaml -n upm-messaging --create-namespace

# 배포 상태 확인
kubectl get all -n upm-messaging -l app.kubernetes.io/name=tcp-bridge

# 로그 확인
kubectl logs -n upm-messaging -l app.kubernetes.io/name=tcp-bridge -f
```

### 3. ServiceMonitor 포함 배포

```bash
# Prometheus Operator가 설치되어 있는지 확인
kubectl get crd servicemonitors.monitoring.coreos.com

# values 파일 수정
cp chart/values.yaml my-monitoring-values.yaml
vi my-monitoring-values.yaml

# 예시:
# serviceMonitor.enabled=true
# serviceMonitor.namespace=upm-monitoring

helm install tcp-bridge chart/ -f my-monitoring-values.yaml -n upm-messaging --create-namespace

# ServiceMonitor 확인
kubectl get servicemonitor -n upm-monitoring tcp-bridge
```

### 4. 프로덕션 배포

```bash
# 프로덕션 values 파일 수정
cp chart/values.yaml my-prod-values.yaml

# 편집 (이미지 레지스트리, TCP 엔드포인트, NATS URL 등)
vi my-prod-values.yaml

# 배포
helm install tcp-bridge chart/ \
  -f my-prod-values.yaml \
  -n production \
  --create-namespace

# 배포 상태 확인
helm status tcp-bridge -n production
kubectl get pods -n production -l app.kubernetes.io/name=tcp-bridge -w
```

### 5. 설정 변경 및 업그레이드

```bash
# values 파일 수정 후 업그레이드
vi my-prod-values.yaml

helm upgrade tcp-bridge chart/ \
  -f my-prod-values.yaml \
  -n production

# 롤백 (문제 발생 시)
helm rollback tcp-bridge -n production
```

### 6. 메트릭 확인

```bash
# Port-forward로 메트릭 접근
kubectl port-forward -n upm-messaging svc/tcp-bridge 8080:8080

# 다른 터미널에서 메트릭 확인
curl http://localhost:8080/metrics
```

### 7. 삭제

```bash
# Chart 삭제
helm uninstall tcp-bridge -n upm-messaging

# 네임스페이스도 삭제하려면
kubectl delete namespace upm-messaging
```

## K3s 환경 특화 설정

현재 환경에 맞는 설정:

```yaml
# my-k3s-values.yaml
replicaCount: 2

serviceMonitor:
  enabled: true
  labels:
    release: kube-prometheus-stack  # 기존 Prometheus Operator 레이블
  namespace: upm-monitoring  # ServiceMonitor는 monitoring 네임스페이스에
  interval: 10s

config:
  nats:
    urls:
      - "nats://nats.upm-messaging.svc.cluster.local:4222"
  
  tcp:
    endpoints:
      - host: "your-tcp-server-host"
        port: 8000
        priority: 0

multus:
  enabled: true
  createNAD: true
  network:
    name: tcp-bridge-net
    master: eth0
    ipam:
      range: 10.10.10.20/29
      rangeStart: 10.10.10.21
      rangeEnd: 10.10.10.22
      gateway: 10.10.10.1
```

배포:

```bash
helm install tcp-bridge chart/ \
  -f my-k3s-values.yaml \
  -n upm-messaging \
  --create-namespace
```

## 유용한 명령어

```bash
# Chart 정보 확인
helm show chart chart/

# 모든 values 확인
helm show values chart/

# 배포된 릴리즈 확인
helm list -A

# 특정 릴리즈 상세 정보
helm get all tcp-bridge -n upm-messaging

# 렌더링된 values 확인
helm get values tcp-bridge -n upm-messaging

# 히스토리 확인
helm history tcp-bridge -n upm-messaging
```

## Troubleshooting

### Pod가 시작되지 않을 때

```bash
# Pod 상태 확인
kubectl describe pod -n upm-messaging -l app.kubernetes.io/name=tcp-bridge

# 로그 확인
kubectl logs -n upm-messaging -l app.kubernetes.io/name=tcp-bridge --previous

# ConfigMap 확인
kubectl get configmap -n upm-messaging tcp-bridge-config -o yaml
```

### ServiceMonitor 작동 확인

```bash
# ServiceMonitor 존재 확인
kubectl get servicemonitor -n upm-monitoring

# Prometheus가 타겟을 발견했는지 확인
kubectl port-forward -n upm-monitoring svc/kube-prometheus-stack-prometheus 9090:9090
# 브라우저: http://localhost:9090/targets

# Prometheus 로그 확인
kubectl logs -n upm-monitoring -l app.kubernetes.io/name=prometheus
```

### NATS 연결 문제

```bash
# NATS 서비스 확인
kubectl get svc -n upm-messaging nats

# NATS 연결 테스트
kubectl run -it --rm debug --image=alpine --restart=Never -- sh
# apk add curl
# curl -v http://nats.upm-messaging.svc.cluster.local:8222/varz
```

## 다음 단계

1. **이미지 빌드**: Docker 이미지를 빌드하고 레지스트리에 push
2. **설정 조정**: values 파일에서 실제 TCP 서버 주소 설정
3. **모니터링 설정**: Grafana 대시보드 구성
4. **알림 설정**: Prometheus AlertManager 규칙 추가
