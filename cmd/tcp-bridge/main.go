package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tcp-bridge/internal/alerta"
	"tcp-bridge/internal/config"
	"tcp-bridge/internal/connection"
	"tcp-bridge/internal/inflight"
	"tcp-bridge/internal/metrics"
	"tcp-bridge/internal/nats"
	"tcp-bridge/internal/tcp"
	"tcp-bridge/internal/worker"
)

// App은 TCP Bridge 메인 애플리케이션을 나타냅니다.
//
// 아키텍처:
// 1. ConnectionManager: 우선순위 기반 다중 TCP 연결 관리 (conn_0 → conn_1 → conn_2 ...)
// 2. NATS Client: Core NATS Queue Group 기반 메시지 수신/전송
// 3. TCP Sender/Reader: 프레임 기반 통신 (Type + Length + TID + Payload)
// 4. MessageHandler: NATS↔TCP 방향 메시지 처리
// 5. InflightManager: 요청-응답 매칭 (TID 기반)
// 6. Metrics: Prometheus 메트릭 수집 및 노출
//
// 라이프사이클:
// Start: metrics → NATS → inflight → connection → TCP reader → message handler
// Stop: message handler → TCP reader → connection → inflight → NATS → metrics
//
// 종료 처리: SIGINT/SIGTERM 시그널을 받아 Graceful Shutdown 수행
type App struct {
	config *config.Config
	logger *slog.Logger

	// Core components
	connMgr     *connection.ConnectionManager // 우선순위 기반 TCP 연결 관리
	natsClient  *nats.Client                  // NATS Queue Group 클라이언트
	inflightMgr *inflight.InflightManager     // 요청-응답 매칭 관리

	// TCP components
	tcpSender *tcp.Sender // TCP 프레임 전송 (큐 없이 직접 전송)
	tcpReader *tcp.Reader // TCP 프레임 수신 (콜백 기반)

	// Message Handlers
	bridgeHandler   *worker.BridgeHandler   // 양방향 브리지 핸들러
	frameDispatcher *worker.FrameDispatcher // TCP 요청 프레임 디스패처

	// Metrics
	metrics *metrics.Metrics // Prometheus 메트릭

	// Alerta
	alertaClient *alerta.Client // Alerta 알람 클라이언트
}

func main() {
	// Parse command line arguments
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger := setupLogger(cfg.Logging)

	// Create application
	app := NewApp(cfg, logger)

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start application
	if err := app.Start(ctx); err != nil {
		logger.Error("failed to start application", "error", err)
		cancel()
		os.Exit(1)
	}

	logger.Info("tcp-bridge started successfully")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("received shutdown signal")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := app.Stop(shutdownCtx); err != nil {
		logger.Error("error during shutdown", "error", err)
	} else {
		logger.Info("tcp-bridge stopped successfully")
	}

	cancel()
}

// generateSysID generates the final SysID by combining sys_prefix_id with Pod index
// Priority:
// 1. POD_INDEX env var (for Deployment with explicit index)
// 2. POD_NAME parsing (for StatefulSet compatibility, e.g., tcp-bridge-0 -> 0)
// 3. sys_prefix_id as-is (for local development)
func (a *App) generateSysID() string {
	// 1. Check POD_INDEX first (Deployment with explicit index)
	podIndex := os.Getenv("POD_INDEX")
	if podIndex != "" {
		// Validate it's a number
		if _, err := strconv.Atoi(podIndex); err != nil {
			a.logger.Warn("POD_INDEX is not numeric, falling back to POD_NAME",
				"pod_index", podIndex)
		} else {
			sysID := fmt.Sprintf("%s%s", a.config.TCP.SysPrefixID, podIndex)
			a.logger.Info("generated SysID from POD_INDEX",
				"sys_id", sysID, "pod_index", podIndex)
			return sysID
		}
	}

	// 2. Fallback: Extract index from POD_NAME (StatefulSet compatibility)
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		// No POD_INDEX and POD_NAME, use prefix as-is (for local development)
		a.logger.Warn("POD_INDEX and POD_NAME not set, using sys_prefix_id as SysID",
			"sys_prefix_id", a.config.TCP.SysPrefixID)
		return a.config.TCP.SysPrefixID
	}

	// Extract StatefulSet index from pod name (e.g., "tcp-bridge-0" -> "0")
	// StatefulSet pod naming: <statefulset-name>-<ordinal>
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		// Not a StatefulSet pod, use prefix as-is
		a.logger.Warn("POD_NAME does not match StatefulSet pattern, using sys_prefix_id",
			"pod_name", podName, "sys_prefix_id", a.config.TCP.SysPrefixID)
		return a.config.TCP.SysPrefixID
	}

	// Last part should be the index
	index := parts[len(parts)-1]

	// Validate it's a number
	if _, err := strconv.Atoi(index); err != nil {
		// Not a valid index, use prefix as-is
		a.logger.Warn("POD_NAME index is not numeric, using sys_prefix_id",
			"pod_name", podName, "index", index, "sys_prefix_id", a.config.TCP.SysPrefixID)
		return a.config.TCP.SysPrefixID
	}

	// Combine prefix with index
	sysID := fmt.Sprintf("%s-%s", a.config.TCP.SysPrefixID, index)
	a.logger.Info("generated SysID from POD_NAME",
		"sys_id", sysID, "pod_name", podName, "index", index)
	return sysID
}

// NewApp creates a new application instance
func NewApp(cfg *config.Config, logger *slog.Logger) *App {
	app := &App{
		config: cfg,
		logger: logger,
	}

	// Initialize components
	app.initializeComponents()

	return app
}

// initializeComponents는 모든 애플리케이션 컴포넨트를 초기화합니다.
//
// 초기화 순서:
// 1. Metrics: Prometheus HTTP 서버 준비
// 2. ConnectionManager: 우선순위 기반 TCP 연결 관리자 생성
// 3. NATS Client: Queue Group 클라이언트 생성
// 4. InflightManager: Inflight-A/B 관리자 생성
// 5. TCP Sender/Reader: 프레임 송수신 컴포넨트 생성
// 6. BridgeHandler: 양방향 브리지 핸들러 생성
// 7. FrameDispatcher: TCP 프레임 디스패처 생성
// 8. Callback 등록: TCP Reader에 REQUEST/RESPONSE 프레임 콜백 설정
//
// 주의: 실제 start는 Start() 함수에서 수행되므로 여기서는 객체 생성만 합니다.
func (a *App) initializeComponents() {
	// Create metrics
	a.metrics = metrics.NewMetrics(a.logger.With("component", "metrics"), &a.config.Metrics)

	// Create connection manager
	a.connMgr = connection.NewConnectionManager(a.logger.With("component", "conn-manager"), &a.config.TCP)

	// Generate SysID from sys_prefix_id + StatefulSet index
	sysID := a.generateSysID()
	a.connMgr.SetSysID(sysID)
	a.logger.Info("SysID configured", "sys_id", sysID)

	a.metrics.SetKeyListProvider(a.connMgr.ListHandshakeResponses)

	// 2026-03-27: Alerta 알람 클라이언트 생성 및 연결 상태 콜백 연동
	a.alertaClient = alerta.NewClient(a.logger.With("component", "alerta"), &a.config.Alerta, sysID)

	// connID → priority 매핑 (알람 전송 시 priority 정보 포함용)
	connPriority := make(map[string]int, len(a.config.TCP.Endpoints))
	for _, ep := range a.config.TCP.Endpoints {
		connPriority[fmt.Sprintf("conn_%d", ep.Priority)] = ep.Priority
	}

	// Set connection state change callback for metrics
	a.connMgr.SetStateChangeCallback(func(connID string, endpoint string, state string) {
		a.metrics.SetConnectionState(connID, endpoint, state)
		// Alerta 알람 전송
		priority := connPriority[connID]
		a.alertaClient.SendConnectionStateAlert(connID, endpoint, state, priority)
	})
	a.connMgr.SetActiveConnectionChangeCallback(func(from string, to string, reason string) {
		a.metrics.SetActiveConnection(connectionIDs(a.connMgr.GetConnections()), normalizeActiveConnectionID(to))
	})
	a.initializeConnectionMetrics()
	a.metrics.SetActiveConnection(connectionIDs(a.connMgr.GetConnections()), "")

	// Create NATS client
	a.natsClient = nats.NewClient(a.logger.With("component", "nats"), &a.config.NATS)

	// Create inflight manager
	a.inflightMgr = inflight.NewInflightManager(a.logger.With("component", "inflight"))

	// Create TCP sender and reader
	a.tcpSender = tcp.NewSender(a.logger.With("component", "tcp-sender"), &a.config.TCP, a.connMgr)
	a.tcpReader = tcp.NewReader(
		a.logger.With("component", "tcp-reader"),
		&a.config.TCP,
		&a.config.NATS.MessageTypeRouting,
		a.connMgr,
	)

	// Create bridge handler
	a.bridgeHandler = worker.NewBridgeHandler(
		a.logger.With("component", "bridge-handler"),
		&a.config.MessageHandler.Outbound,
		&a.config.MessageHandler.Inbound,
		&a.config.NATS,
		a.natsClient,
		a.tcpSender,
		a.inflightMgr,
		a.connMgr, // Priority 기반 재시도용
		a.metrics,
	)

	// Create frame dispatcher
	a.frameDispatcher = worker.NewFrameDispatcher(
		a.logger.With("component", "frame-dispatcher"),
		a.bridgeHandler,
	)

	// Set up TCP reader callbacks
	a.tcpReader.SetRequestFrameCallback(a.handleTCPRequest)
	a.tcpReader.SetResponseFrameCallback(a.handleTCPResponse)
}

// Start starts all application components
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("starting tcp-bridge application", "version", a.config.Server.Version)

	// Start metrics server
	if err := a.metrics.Start(); err != nil {
		return fmt.Errorf("failed to start metrics: %w", err)
	}

	// Connect to NATS
	if err := a.natsClient.Connect(); err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Start inflight manager
	if err := a.inflightMgr.Start(ctx); err != nil {
		return fmt.Errorf("failed to start inflight manager: %w", err)
	}

	// Start connection manager
	if err := a.connMgr.Start(ctx); err != nil {
		return fmt.Errorf("failed to start connection manager: %w", err)
	}

	// Start TCP reader
	if err := a.tcpReader.Start(ctx); err != nil {
		return fmt.Errorf("failed to start TCP reader: %w", err)
	}

	// Start bridge handler
	if err := a.bridgeHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start bridge handler: %w", err)
	}

	return nil
}

// Stop gracefully stops all application components
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("stopping tcp-bridge application")

	// Stop bridge handler to stop processing new requests
	a.bridgeHandler.Stop()

	// Stop TCP components
	a.tcpReader.Stop()

	// Graceful shutdown: 프로세스 다운 알림 전송 (비동기)
	for i, conn := range a.connMgr.GetConnections() {
		if conn != nil {
			if err := a.alertaClient.SendShutdownAlert(ctx, conn.ID(), conn.GetEndpointAddress(), i); err != nil {
				a.logger.Warn("failed to send shutdown alert",
					"conn_id", conn.ID(),
					"endpoint", conn.GetEndpointAddress(),
					"priority", i,
					"error", err)
			}
		}
	}

	// Stop connection manager
	a.connMgr.Stop()

	// Stop inflight manager
	a.inflightMgr.Stop()

	// Disconnect from NATS
	a.natsClient.Disconnect()

	// Stop metrics server
	if err := a.metrics.Stop(); err != nil {
		a.logger.Error("error stopping metrics server", "error", err)
	}

	return nil
}

// handleTCPRequest handles incoming TCP request frames
func (a *App) handleTCPRequest(frame *config.Frame) {
	frame.ReceivedAt = time.Now()

	msgType := fmt.Sprintf("0x%02x", frame.Type)
	a.logger.Info("inbound TCP request received",
		"direction", "inbound",
		"stage", "tcp_request_received",
		"tid", frame.TID,
		"connection_id", frame.ConnectionID,
		"msg_type", msgType,
		"frame", frame.String(),
		"payload_size", len(frame.Payload))
	a.metrics.IncInboundRequests(frame.ConnectionID, msgType)

	// Dispatch to bridge handler via frame dispatcher
	a.frameDispatcher.DispatchRequestFrame(frame)
}

// handleTCPResponse handles incoming TCP response frames
func (a *App) handleTCPResponse(frame *config.Frame) {
	a.logger.Info("outbound TCP response received",
		"direction", "outbound",
		"stage", "tcp_response_received",
		"tid", frame.TID,
		"connection_id", frame.ConnectionID,
		"msg_type", fmt.Sprintf("0x%02x", frame.Type),
		"frame", frame.String(),
		"payload_size", len(frame.Payload))

	// Match with inflight-A entries (NATS→TCP responses)
	if handled := a.inflightMgr.GetInflightA().HandleResponse(frame); !handled {
		a.logger.Warn("dropping unmatched TCP response",
			"tid", frame.TID,
			"connection_id", frame.ConnectionID,
			"response_type", fmt.Sprintf("0x%02x", frame.Type),
			"payload_size", len(frame.Payload))
	}
}

func (a *App) initializeConnectionMetrics() {
	for _, conn := range a.connMgr.GetConnections() {
		if conn == nil {
			continue
		}
		a.metrics.SetConnectionState(conn.ID(), conn.GetEndpointAddress(), conn.GetState())
	}
}

func connectionIDs(connections []*connection.ConnMgr) []string {
	ids := make([]string, 0, len(connections))
	for _, conn := range connections {
		if conn != nil {
			ids = append(ids, conn.ID())
		}
	}
	return ids
}

func normalizeActiveConnectionID(connectionID string) string {
	if connectionID == "" || connectionID == "nil" {
		return ""
	}
	return connectionID
}

// setupLogger creates a structured logger
func setupLogger(cfg config.LoggingConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
