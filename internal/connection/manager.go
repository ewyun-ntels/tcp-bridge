package connection

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/tid"
)

// ConnectionManager는 우선순위 기반 장애조치(failover)를 지원하는 여러 TCP 연결을 관리합니다.
// 각 연결은 priority를 가지며, 가장 낮은 priority 값을 가진 READY 상태의 연결이 활성 연결로 선택됩니다.
//
// 동시성: ConnectionManager는 thread-safe하며, 여러 고루틴에서 안전하게 사용할 수 있습니다.
type ConnectionManager struct {
	logger *slog.Logger
	config *config.TCPConfig
	store  *HandshakeStore

	// SysID: StatefulSet index가 결합된 최종 Peer Name
	sysID string

	// 우선순위 순서로 정렬된 연결 관리자 배열
	// connections[i]는 priority i를 가진 연결입니다.
	connections []*ConnMgr

	// 활성 연결을 선택하고 관리하는 selector
	selector *PriorityConnSelector

	// 연결 상태 및 품질 변경 시 호출되는 콜백 (메트릭 수집 등에 사용)
	onStateChange      func(connID string, endpoint string, state string)
	onHandshakeAck     func(connID string, endpoint string, sysID string)
	onConnectAttempt   func(connID string)
	onConnectSuccess   func(connID string)
	onConnectFailure   func(connID string, reason string)
	onDisconnect       func(connID string, reason string)
	onPingRTT          func(connID string, duration time.Duration)
	onActiveConnChange func(from string, to string, reason string)
}

// ConnMgr는 단일 TCP 연결을 상태 머신(state machine)과 함께 관리합니다.
// 연결 상태: DISCONNECTED → CONNECTING → READY → DISCONNECTED
//
// 동시성: ConnMgr는 내부적으로 mutex를 사용하여 상태와 연결을 보호합니다.
// 단, 콜백 함수는 lock 없이 호출되므로 callback 안에서 deadlock을 유발하지 않도록 주의해야 합니다.
type ConnMgr struct {
	id       string // 연결 식별자 (예: "conn_0")
	logger   *slog.Logger
	config   *config.TCPConfig
	endpoint config.TCPEndpoint // 연결할 TCP 엔드포인트

	// SysID: StatefulSet index가 결합된 최종 Peer Name
	sysID string

	// 연결 상태 (DISCONNECTED, CONNECTING, READY)
	mu    sync.RWMutex
	state string
	conn  net.Conn

	// Lifecycle control
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// Ping control
	pingInterval time.Duration // 서버로부터 받은 ping 주기
	pingTimeout  time.Duration // PING 전송 후 PONG 대기 timeout
	pingDone     chan struct{} // ping 고루틴 종료 신호

	// Cleanup guard
	cleanupOnce sync.Once

	// Activity tracking (for idle-based ping)
	activityMu       sync.Mutex
	lastActivityTime time.Time // 마지막 메시지 송수신 시간
	awaitingPong     bool      // PONG 대기 중 플래그
	lastPingSentAt   time.Time
	disconnectReason string

	// Callbacks (lock 없이 호출됨 - deadlock 주의!)
	onStateChange       func(state string)
	onFrameRead         func(frame *config.Frame)
	onHandshakeResponse func(sysID string, payload json.RawMessage)
	onConnectAttempt    func()
	onConnectSuccess    func()
	onConnectFailure    func(reason string)
	onDisconnect        func(reason string)
	onPingRTT           func(duration time.Duration)
}

// ID returns the stable connection identifier.
func (c *ConnMgr) ID() string {
	return c.id
}

// PriorityConnSelector는 우선순위 기반으로 활성 연결을 선택합니다.
// 가장 낮은 priority 값을 가진 READY 상태의 연결이 활성 연결로 선택됩니다.
//
// 동시성: thread-safe하며, ReEvaluate()는 연결 상태가 변경될 때마다 자동으로 호출됩니다.
type PriorityConnSelector struct {
	logger *slog.Logger

	mu             sync.RWMutex
	connections    []*ConnMgr // 우선순위 순서로 정렬된 연결 목록
	active         *ConnMgr   // 현재 활성 연결
	activePriority int        // 현재 활성 연결의 우선순위 (-1은 활성 연결 없음)
	onActiveChange func(from string, to string, reason string)
}

// NewConnectionManager는 새로운 연결 관리자를 생성합니다.
// 모든 endpoint에 대해 ConnMgr를 생성하고, priority 순서대로 정렬합니다.
//
// 주의사항:
//   - Endpoint의 priority 값은 0부터 len(endpoints)-1 사이어야 합니다.
//   - 중복된 priority가 있으면 나중 것이 이전 것을 덮어씁니다.
func NewConnectionManager(logger *slog.Logger, cfg *config.TCPConfig) *ConnectionManager {
	cm := &ConnectionManager{
		logger:      logger,
		config:      cfg,
		store:       NewHandshakeStore(),
		connections: make([]*ConnMgr, len(cfg.Endpoints)),
	}

	// 각 endpoint에 대해 연결 관리자 생성 (priority 순서대로)
	for i, endpoint := range cfg.Endpoints {
		connID := fmt.Sprintf("conn_%d", endpoint.Priority)
		//endpointAddr := fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
		conn := NewConnMgr(connID, logger.With(
			"conn", connID,
			"priority", endpoint.Priority,
		), cfg, endpoint)
		conn.onHandshakeResponse = func(sysID string, payload json.RawMessage) {
			cm.store.Save(sysID, payload)
			if cm.onHandshakeAck != nil {
				cm.onHandshakeAck(connID, endpoint.Address(), sysID)
			}
		}

		// Priority 순서대로 저장 (index = priority)
		if endpoint.Priority >= 0 && endpoint.Priority < len(cfg.Endpoints) {
			cm.connections[endpoint.Priority] = conn
		} else {
			// Priority가 범위를 벗어나면 순차적으로 저장 (fallback)
			cm.connections[i] = conn
		}

		// 상태 변경 콜백 설정
		// 상태가 변경되면 메트릭을 업데이트하고, selector를 재평가합니다.
		conn.onStateChange = func(state string) {
			if cm.onStateChange != nil {
				cm.onStateChange(connID, endpoint.Address(), state)
			}
			// 활성 연결 재선택 (더 높은 우선순위 연결이 READY 상태가 되었을 수 있음)
			cm.selector.ReEvaluate()
		}
		conn.onConnectAttempt = func() {
			if cm.onConnectAttempt != nil {
				cm.onConnectAttempt(connID)
			}
		}
		conn.onConnectSuccess = func() {
			if cm.onConnectSuccess != nil {
				cm.onConnectSuccess(connID)
			}
		}
		conn.onConnectFailure = func(reason string) {
			if cm.onConnectFailure != nil {
				cm.onConnectFailure(connID, reason)
			}
		}
		conn.onDisconnect = func(reason string) {
			if cm.onDisconnect != nil {
				cm.onDisconnect(connID, reason)
			}
		}
		conn.onPingRTT = func(duration time.Duration) {
			if cm.onPingRTT != nil {
				cm.onPingRTT(connID, duration)
			}
		}
	}

	// 우선순위 기반 연결 selector 생성
	cm.selector = NewPriorityConnSelector(logger, cm.connections)
	cm.selector.onActiveChange = func(from string, to string, reason string) {
		if cm.onActiveConnChange != nil {
			cm.onActiveConnChange(from, to, reason)
		}
	}

	return cm
}

// NewConnMgr는 단일 TCP 연결을 관리하는 ConnMgr를 생성합니다.
// 초기 상태는 DISCONNECTED이며, Start() 호출 시 자동으로 연결을 시도합니다.
//
// Parameters:
//   - id: 연결 식별자 (로그 및 메트릭에 사용)
//   - logger: 로거 인스턴스
//   - cfg: TCP 설정
//   - endpoint: 연결할 TCP 엔드포인트 정보
func NewConnMgr(id string, logger *slog.Logger, cfg *config.TCPConfig, endpoint config.TCPEndpoint) *ConnMgr {
	return &ConnMgr{
		id:          id,
		logger:      logger,
		config:      cfg,
		endpoint:    endpoint,
		state:       config.ConnStateDisconnected,
		done:        make(chan struct{}),
		pingTimeout: cfg.PingTimeout,
	}
}

// NewPriorityConnSelector는 우선순위 기반 연결 selector를 생성합니다.
// 초기에는 활성 연결이 없으며 (activePriority=-1), ReEvaluate() 호출 시
// READY 상태인 연결 중 가장 낮은 priority를 가진 연결이 활성 연결로 선택됩니다.
func NewPriorityConnSelector(logger *slog.Logger, connections []*ConnMgr) *PriorityConnSelector {
	return &PriorityConnSelector{
		logger:         logger,
		connections:    connections,
		activePriority: -1, // 초기에는 활성 연결 없음
	}
}

// Start는 모든 연결 관리자를 시작합니다.
// 각 연결은 백그라운드에서 자동으로 재연결을 시도합니다.
//
// 에러 처리:
//   - 하나의 연결 시작이 실패하면, 이미 시작된 모든 연결을 중지하고 에러를 반환합니다.
//   - 부분적 실패는 허용하지 않습니다 (all-or-nothing).
//
// 동시성: 모든 연결이 독립적인 고루틴에서 실행되므로 병렬로 동작합니다.
func (cm *ConnectionManager) Start(ctx context.Context) error {
	cm.logger.Info("starting connection manager", "total_connections", len(cm.connections))

	// 모든 연결 시작
	for i, conn := range cm.connections {
		if conn != nil {
			if err := conn.Start(ctx); err != nil {
				// 실패 시 이미 시작된 연결 모두 중지 (cleanup)
				for j := 0; j < i; j++ {
					if cm.connections[j] != nil {
						cm.connections[j].Stop()
					}
				}
				return fmt.Errorf("failed to start connection %d: %w", i, err)
			}
		}
	}

	cm.logger.Info("connection manager started")
	return nil
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	cm.logger.Info("stopping connection manager")

	// Stop all connections
	for i, conn := range cm.connections {
		if conn != nil {
			cm.logger.Debug("stopping connection", "priority", i)
			conn.Stop()
		}
	}

	cm.logger.Info("connection manager stopped")
}

// GetActiveConn은 현재 활성화된 TCP 연결을 반환합니다.
// 활성 연결이 없으면 nil을 반환합니다.
//
// Thread-safe: 여러 고루틴에서 동시에 호출할 수 있습니다.
//
// 사용 예시:
//
//	conn := cm.GetActiveConn()
//	if conn == nil {
//	    return errors.New("no ready connection available")
//	}
//	conn.Write(data)
func (cm *ConnectionManager) GetActiveConn() net.Conn {
	return cm.selector.GetActiveConn()
}

// Write sends data through the active connection and updates activity time.
// Returns error if no active connection is available.
//
// Thread-safe: 여러 고루틴에서 동시에 호출할 수 있습니다.
func (cm *ConnectionManager) Write(data []byte) (int, error) {
	active := cm.selector.GetActive()
	if active == nil {
		return 0, fmt.Errorf("no ready connection available")
	}
	return active.Write(data)
}

// WriteToConnection sends data to a specific connection by ID.
// Returns error if connection is not found or not ready.
//
// Thread-safe: 여러 고루틴에서 동시에 호출할 수 있습니다.
func (cm *ConnectionManager) WriteToConnection(connectionID string, data []byte) (int, error) {
	// Find connection by ID
	for _, conn := range cm.connections {
		if conn != nil && conn.id == connectionID {
			if conn.GetState() != config.ConnStateReady {
				return 0, fmt.Errorf("connection %s is not ready (state: %s)", connectionID, conn.GetState())
			}
			return conn.Write(data)
		}
	}
	return 0, fmt.Errorf("connection %s not found", connectionID)
}

// GetConnections returns all connection managers (priority ordered).
// Returns a slice where index corresponds to priority.
//
// Thread-safe: 여러 고루틴에서 동시에 호출할 수 있습니다.
func (cm *ConnectionManager) GetConnections() []*ConnMgr {
	return cm.connections
}

// GetConnectionByPriority returns the connection manager for a specific priority.
// Returns nil if priority is out of range.
//
// Thread-safe: 여러 고루틴에서 동시에 호출할 수 있습니다.
func (cm *ConnectionManager) GetConnectionByPriority(priority int) *ConnMgr {
	if priority < 0 || priority >= len(cm.connections) {
		return nil
	}
	return cm.connections[priority]
}

// SetFrameReadCallback은 프레임 읽기 콜백을 설정합니다.
// 모든 연결에서 프레임을 수신하면 이 콜백이 호출됩니다.
// Frame.ConnectionID로 endpoint별 구분하므로 모든 connection에서 처리합니다.
//
// 주의사항:
//   - 콜백은 readLoop 고루틴에서 직접 호출되므로, 블로킹 작업을 하면 안 됩니다.
//   - 긴 처리가 필요한 경우, 별도 고루틴으로 전달하세요.
//   - 콜백 내에서 lock을 잡을 때 deadlock에 주의하세요.
//
// 사용 예시:
//
//	cm.SetFrameReadCallback(func(frame *config.Frame) {
//	    // frame.ConnectionID로 endpoint 확인
//	    go handleFrameAsync(frame) // 긴 처리는 별도 고루틴에서
//	})
func (cm *ConnectionManager) SetFrameReadCallback(callback func(frame *config.Frame)) {
	// 모든 연결에 콜백 설정 (Frame.ConnectionID로 endpoint 구분)
	for _, conn := range cm.connections {
		if conn != nil {
			conn.onFrameRead = callback
		}
	}
}

// SetStateChangeCallback sets the callback for state changes
func (cm *ConnectionManager) SetStateChangeCallback(callback func(connID string, endpoint string, state string)) {
	cm.onStateChange = callback
}

func (cm *ConnectionManager) SetHandshakeAckCallback(callback func(connID string, endpoint string, sysID string)) {
	cm.onHandshakeAck = callback
}

func (cm *ConnectionManager) SetConnectAttemptCallback(callback func(connID string)) {
	cm.onConnectAttempt = callback
}

func (cm *ConnectionManager) SetConnectSuccessCallback(callback func(connID string)) {
	cm.onConnectSuccess = callback
}

func (cm *ConnectionManager) SetConnectFailureCallback(callback func(connID string, reason string)) {
	cm.onConnectFailure = callback
}

func (cm *ConnectionManager) SetDisconnectCallback(callback func(connID string, reason string)) {
	cm.onDisconnect = callback
}

func (cm *ConnectionManager) SetPingRTTCallback(callback func(connID string, duration time.Duration)) {
	cm.onPingRTT = callback
}

func (cm *ConnectionManager) SetActiveConnectionChangeCallback(callback func(from string, to string, reason string)) {
	cm.onActiveConnChange = callback
}

// SetSysID sets the SysID for all connections
// This should be called before Start() to set the dynamically generated SysID (prefix + StatefulSet index)
func (cm *ConnectionManager) SetSysID(sysID string) {
	cm.sysID = sysID
	for _, conn := range cm.connections {
		if conn != nil {
			conn.sysID = sysID
		}
	}
}

// ListHandshakeResponses returns the latest HELLO responses keyed by peer sys-id.
func (cm *ConnectionManager) ListHandshakeResponses() []json.RawMessage {
	return cm.store.List()
}

// Start starts the connection manager
func (c *ConnMgr) Start(ctx context.Context) error {
	c.logger.Info("starting connection manager", "endpoint", c.endpoint.Address())

	if ctx == nil {
		ctx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	// Start connection loop
	go c.connectionLoop()

	return nil
}

// Stop stops the connection manager
func (c *ConnMgr) Stop() {
	c.logger.Info("stopping connection manager")

	if c.cancel != nil {
		c.cancel()
	}

	// Wait for connection loop to exit
	select {
	case <-c.done:
		c.logger.Info("connection manager stopped")
	case <-time.After(5 * time.Second):
		c.logger.Warn("connection manager stop timeout")
	}
}

// GetConn returns the current connection (may be nil)
func (c *ConnMgr) GetConn() net.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// GetState returns the current connection state
func (c *ConnMgr) GetState() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *ConnMgr) GetEndpointAddress() string {
	return c.endpoint.Address()
}

func (c *ConnMgr) GetLastActivityTime() time.Time {
	c.activityMu.Lock()
	defer c.activityMu.Unlock()
	return c.lastActivityTime
}

// setState sets the connection state and triggers callback
func (c *ConnMgr) setState(newState string) {
	c.mu.Lock()
	oldState := c.state
	c.state = newState
	c.mu.Unlock()

	if oldState != newState {
		c.logger.Info("connection state changed", "from", oldState, "to", newState, "endpoint", c.endpoint.Address())
		if c.onStateChange != nil {
			c.onStateChange(newState)
		}
	}
}

// connectionLoop manages the connection lifecycle
func (c *ConnMgr) connectionLoop() {
	defer close(c.done)

	ticker := time.NewTicker(5 * time.Second) // Reconnect interval
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			c.cleanupConnection()
			return

		case <-ticker.C:
			// Drain any buffered ticks to prevent immediate retry
			select {
			case <-ticker.C:
			default:
			}

			state := c.GetState()
			if state == config.ConnStateDisconnected || state == config.ConnStateConnectFailed {
				c.attemptConnection()
			}
		}
	}
}

// attemptConnection attempts to establish a connection
func (c *ConnMgr) attemptConnection() {
	if c.onConnectAttempt != nil {
		c.onConnectAttempt()
	}
	c.setState(config.ConnStateConnecting)

	// Establish TCP connection
	conn, err := net.DialTimeout("tcp", c.endpoint.Address(), c.config.ConnectTimeout)
	if err != nil {
		c.logger.Error("failed to connect", "error", err)
		if c.onConnectFailure != nil {
			c.onConnectFailure(classifyDialError(err))
		}
		c.setState(config.ConnStateConnectFailed)
		return
	}

	// Set connection options
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(c.config.KeepAlive)
	}

	// Store connection (reset cleanup guard for new connection lifecycle)
	c.mu.Lock()
	c.conn = conn
	c.cleanupOnce = sync.Once{}
	c.mu.Unlock()

	// Perform handshake
	if err := c.performHandshake(); err != nil {
		c.logger.Error("handshake failed", "error", err)
		c.cleanupConnection()
		if c.onConnectFailure != nil {
			c.onConnectFailure(classifyHandshakeError(err))
		}
		c.setState(config.ConnStateConnectFailed)
		return
	}

	// Connection ready
	c.setState(config.ConnStateReady)
	if c.onConnectSuccess != nil {
		c.onConnectSuccess()
	}

	// Initialize activity time
	c.updateLastActivity()

	// Start ping loop if ping interval was set
	c.mu.RLock()
	pingInterval := c.pingInterval
	c.mu.RUnlock()

	if pingInterval > 0 {
		c.pingDone = make(chan struct{})
		go c.pingLoop()
	}

	// Start reading frames
	go c.readLoop()
}

// performHandshake performs Hello/ACK handshake with JSON-based protocol
func (c *ConnMgr) performHandshake() error {
	conn := c.GetConn()
	if conn == nil {
		return fmt.Errorf("no connection available")
	}

	// Prepare HELLO request with sys-id and branch-name
	helloReq := config.HandshakeRequest{
		SysID:      c.sysID, // Use dynamically set SysID
		BranchName: c.config.BranchName,
	}

	helloPayload, err := json.Marshal(helloReq)
	if err != nil {
		return fmt.Errorf("failed to marshal hello request: %w", err)
	}

	// Send HELLO frame
	helloFrame := &config.Frame{
		Type:    config.FrameTypeHello,
		TID:     tid.Next(),
		Payload: helloPayload,
	}

	helloData, err := helloFrame.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize hello frame: %w", err)
	}

	conn.SetWriteDeadline(time.Now().Add(c.config.HandshakeTimeout))
	if _, err := conn.Write(helloData); err != nil {
		return fmt.Errorf("failed to send hello: %w", err)
	}

	c.logger.Info(">>> HELLO-REQ sent", "tid", helloFrame.TID, "sys-id", helloReq.SysID, "branch-name", helloReq.BranchName)

	// Read ACK frame
	conn.SetReadDeadline(time.Now().Add(c.config.HandshakeTimeout))

	// Read frame header (8 bytes, 4.1.1)
	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("failed to read ack header: %w", err)
	}

	// Parse header to determine payload length
	// Byte 1: Extension Bit + Protocol Version + Reserved
	extVersion := (header[0] >> 7) & 0x01
	protocolVer := (header[0] >> 5) & 0x03

	// Validate protocol version
	if protocolVer != 0 {
		return fmt.Errorf("unsupported protocol version: %d (expected 0)", protocolVer)
	}
	if extVersion != 0 {
		return fmt.Errorf("unsupported extension bit: %d (expected 0)", extVersion)
	}

	// Byte 2: Message Type
	frameType := header[1]

	// Byte 3-4: Body Length
	payloadLen := int(binary.BigEndian.Uint16(header[2:4]))

	// Validate ACK frame type
	if frameType != config.FrameTypeAck {
		return fmt.Errorf("expected ACK frame (0x%02x), got type 0x%02x", config.FrameTypeAck, frameType)
	}

	// Read payload
	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return fmt.Errorf("failed to read ack payload: %w", err)
		}
	}

	// Parse only the fields needed for runtime behavior; successful payloads are stored as raw JSON.
	var ackResp struct {
		SysID        string `json:"sys-id"`
		Code         string `json:"code"`
		PingInterval *int   `json:"ping-interval"`
		Cause        string `json:"cause"`
	}
	if err := json.Unmarshal(payload, &ackResp); err != nil {
		return fmt.Errorf("failed to unmarshal ack response: %w", err)
	}

	pingIntervalSec := 5
	if ackResp.PingInterval != nil {
		pingIntervalSec = *ackResp.PingInterval
	}

	// Check response code
	if ackResp.Code != "200" {
		c.logger.Warn("HELLO response rejected",
			"tid", binary.BigEndian.Uint32(header[4:8]),
			"peer-sys-id", ackResp.SysID,
			"code", ackResp.Code,
			"cause", ackResp.Cause,
			"payload", string(payload))
		if ackResp.Cause != "" {
			return fmt.Errorf("handshake failed with code %s: %s", ackResp.Code, ackResp.Cause)
		}
		return fmt.Errorf("handshake failed with code %s", ackResp.Code)
	}

	if c.onHandshakeResponse != nil {
		c.onHandshakeResponse(ackResp.SysID, payload)
	}

	// Apply default ping interval when the optional field is omitted.
	if pingIntervalSec > 0 {
		rawInterval := time.Duration(pingIntervalSec) * time.Second
		effectiveInterval := rawInterval - c.config.PingIntervalMargin
		if effectiveInterval <= 0 {
			effectiveInterval = time.Second
		}

		c.mu.Lock()
		c.pingInterval = effectiveInterval
		c.mu.Unlock()
		c.logger.Info("received ping interval",
			"interval", pingIntervalSec,
			"seconds", pingIntervalSec,
			"default_applied", ackResp.PingInterval == nil,
			"margin", c.config.PingIntervalMargin.String(),
			"effective_interval", effectiveInterval.String())
	}

	// Clear read/write deadlines after handshake (ping-pong will manage connection health)
	conn.SetReadDeadline(time.Time{}) // zero time = no deadline
	conn.SetWriteDeadline(time.Time{})

	c.logger.Info("<<< HELLO-RESP received",
		"tid", binary.BigEndian.Uint32(header[4:8]),
		"peer-sys-id", ackResp.SysID,
		"ping-interval", pingIntervalSec)

	return nil
}

// readLoop continuously reads frames from the connection
func (c *ConnMgr) readLoop() {
	conn := c.GetConn()
	if conn == nil {
		return
	}

	disconnectReason := ""
	defer func() {
		c.cleanupConnection()
		disconnectReason = c.takeDisconnectReason(disconnectReason)
		if disconnectReason != "" && c.onDisconnect != nil {
			c.onDisconnect(disconnectReason)
		}
		c.setState(config.ConnStateDisconnected)
	}()

	for {
		// Check context cancellation
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// ping-pong 메커니즘으로 연결 상태를 관리하므로 read timeout 미설정
		// (ping_timeout으로 실제 연결 끊김 감지)

		// Read frame header (8 bytes, 4.1.1) - use ReadFull to ensure complete read
		// Byte 1: Extension Bit + Protocol Version + Reserved
		// Byte 2: Message Type
		// Byte 3-4: Body Length (Big Endian)
		// Byte 5-8: Transaction Identifier (Big Endian)
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			disconnectReason = classifyReadError(err)
			if err != io.EOF {
				c.logger.Error("failed to read frame header", "error", err)
			}
			return
		}

		// Parse header (4.1.1)
		// Byte 1: Extension Bit + Protocol Version + Reserved
		extVersion := (header[0] >> 7) & 0x01
		protocolVer := (header[0] >> 5) & 0x03

		// Validate protocol version (4.1.1.b.ii)
		if protocolVer != 0 {
			c.logger.Error("unsupported protocol version", "version", protocolVer)
			return
		}

		// Extension bit는 현재 버전에서 0이어야 함 (4.1.1.a.ii)
		if extVersion != 0 {
			c.logger.Error("unsupported extension bit", "ext", extVersion)
			return
		}

		// Byte 2: Message Type (4.1.1.d)
		frameType := header[1]

		// Byte 3-4: Body Length (4.1.1.e)
		payloadLen := int(binary.BigEndian.Uint16(header[2:4]))

		// Byte 5-8: Transaction Identifier (4.1.1.f)
		tid := binary.BigEndian.Uint32(header[4:8])

		// Log header details
		c.logger.Info("TCP frame received",
			"header", fmt.Sprintf("%02X %02X %02X %02X %02X %02X %02X %02X",
				header[0], header[1], header[2], header[3],
				header[4], header[5], header[6], header[7]),
			"byte1", fmt.Sprintf("0x%02X", header[0]),
			"msg_type", fmt.Sprintf("0x%02X", frameType),
			"body_len", payloadLen,
			"tid", tid,
			"connection_id", c.id)

		// Validate payload length
		if payloadLen > c.config.MaxFrameSize {
			c.logger.Error("frame too large", "size", payloadLen, "max", c.config.MaxFrameSize)
			return
		}

		// Read payload
		var payload []byte
		if payloadLen > 0 {
			payload = make([]byte, payloadLen)
			if _, err := io.ReadFull(conn, payload); err != nil {
				disconnectReason = "read_error"
				c.logger.Error("failed to read frame payload", "error", err)
				return
			}
		}

		// Create frame
		frame := &config.Frame{
			Type:         frameType,
			TID:          tid,
			Payload:      payload,
			ConnectionID: c.id, // Set connectionID for endpoint tracking
		}

		// Handle ping/pong frames (do not update activity for keep-alive messages)
		if frame.IsPing() {
			switch frameType {
			case config.FrameTypePing:
				c.logger.Warn("unexpected PING-REQ received", "tid", tid)
			case config.FrameTypePong:
				// Received PONG, clear awaiting flag
				c.activityMu.Lock()
				c.awaitingPong = false
				pingSentAt := c.lastPingSentAt
				c.lastPingSentAt = time.Time{}
				c.activityMu.Unlock()
				if !pingSentAt.IsZero() && c.onPingRTT != nil {
					c.onPingRTT(time.Since(pingSentAt))
				}
				c.logger.Info("<<< PING-RESP received", "tid", tid)
			}
			// Skip further processing for ping/pong frames
			c.logger.Debug("handled ping/pong frame", "type", frameType)
			continue
		}

		// Skip handshake frames during normal operation
		if frame.IsHandshake() {
			c.logger.Debug("skipping handshake frame", "type", frameType)
			continue
		}

		// Update last activity time for data frames only (not ping/pong)
		c.updateLastActivity()

		c.logger.Debug("dispatching frame to callback",
			"type", frameType,
			"tid", tid,
			"has_callback", c.onFrameRead != nil)

		// Call frame read callback
		if c.onFrameRead != nil {
			c.onFrameRead(frame)
		} else {
			c.logger.Warn("no frame read callback set", "tid", tid)
		}
	}
}

// cleanupConnection cleans up the current connection.
// Safe to call from multiple goroutines concurrently; only the first call takes effect.
func (c *ConnMgr) cleanupConnection() {
	c.cleanupOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Stop ping loop if running
		if c.pingDone != nil {
			close(c.pingDone)
			c.pingDone = nil
		}

		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
	})
}

// GetActiveConn returns the currently active connection
func (s *PriorityConnSelector) GetActiveConn() net.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.active != nil {
		return s.active.GetConn()
	}
	return nil
}

// GetActive returns the currently active ConnMgr
func (s *PriorityConnSelector) GetActive() *ConnMgr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// ReEvaluate re-evaluates and selects the best available connection based on priority
func (s *PriorityConnSelector) ReEvaluate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.selectActiveConnection()
}

// selectActiveConnection implements priority-based failover logic
func (s *PriorityConnSelector) selectActiveConnection() {
	var newActive *ConnMgr
	newPriority := -1

	// Find the highest priority (lowest index) READY connection
	for i, conn := range s.connections {
		if conn != nil && conn.GetState() == config.ConnStateReady {
			newActive = conn
			newPriority = i
			break // Found highest priority READY connection
		}
	}

	// Update active connection if changed
	if s.active != newActive {
		oldActive := "nil"
		oldPriority := s.activePriority
		if s.active != nil {
			oldActive = s.active.id
		}

		newActiveID := "nil"
		if newActive != nil {
			newActiveID = newActive.id
		}

		s.active = newActive
		s.activePriority = newPriority

		reason := determineActiveChangeReason(oldPriority, newPriority)
		if s.onActiveChange != nil {
			s.onActiveChange(oldActive, newActiveID, reason)
		}
		s.logger.Info("active connection changed",
			"from", oldActive, "from_priority", oldPriority,
			"to", newActiveID, "to_priority", newPriority,
			"reason", reason)
	}
}

// pingLoop sends PING frames when connection is idle (no messages for pingInterval duration)
func (c *ConnMgr) pingLoop() {
	c.mu.RLock()
	interval := c.pingInterval
	timeout := c.pingTimeout
	c.mu.RUnlock()

	if interval == 0 {
		c.logger.Warn("ping interval is 0, ping loop will not run")
		return
	}

	// Check idle time every 1 second for accurate timing
	checkInterval := 1 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	c.logger.Info("ping loop started", "ping_interval", interval, "check_interval", checkInterval, "ping_timeout", timeout)

	for {
		select {
		case <-c.pingDone:
			c.logger.Info("ping loop stopped")
			return
		case <-c.ctx.Done():
			c.logger.Info("ping loop cancelled")
			return
		case <-ticker.C:
			c.activityMu.Lock()
			idleTime := time.Since(c.lastActivityTime)
			awaitingPong := c.awaitingPong
			c.activityMu.Unlock()

			// Check if PONG timeout occurred
			if awaitingPong && idleTime > timeout {
				c.logger.Error("ping timeout: no PONG received", "timeout", timeout, "idle_time", idleTime)
				// Close connection to trigger reconnection
				c.markDisconnectReason("ping_timeout")
				c.cleanupConnection()
				c.setState(config.ConnStateDisconnected)
				return
			}

			// Send PING only if idle time exceeds ping interval and not already waiting for PONG
			if !awaitingPong && idleTime >= interval {
				c.logger.Info(">>> PING-REQ sending", "idle_time", idleTime)
				if err := c.sendPing(); err != nil {
					c.logger.Error("failed to send ping", "error", err)
					// Close connection to trigger reconnection
					c.markDisconnectReason("write_error")
					c.cleanupConnection()
					c.setState(config.ConnStateDisconnected)
					return
				}

				// Mark as awaiting PONG and update activity time to prevent duplicate PINGs
				c.activityMu.Lock()
				c.awaitingPong = true
				c.lastActivityTime = time.Now() // PING도 마지막 전송 시간으로 기록
				c.lastPingSentAt = c.lastActivityTime
				c.activityMu.Unlock()
			}
		}
	}
}

// sendPing sends a PING frame to the server
func (c *ConnMgr) sendPing() error {
	conn := c.GetConn()
	if conn == nil {
		return fmt.Errorf("no connection available")
	}

	// PING frame has no payload (JSON-less)
	pingFrame := &config.Frame{
		Type:    config.FrameTypePing,
		TID:     tid.Next(),
		Payload: nil,
	}

	pingData, err := pingFrame.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize ping frame: %w", err)
	}

	conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	if _, err := conn.Write(pingData); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	// PING은 keep-alive용이므로 activity time을 갱신하지 않음

	c.logger.Info(">>> PING-REQ sent", "tid", pingFrame.TID)
	return nil
}

// updateLastActivity updates the last activity timestamp
func (c *ConnMgr) updateLastActivity() {
	c.activityMu.Lock()
	c.lastActivityTime = time.Now()
	c.activityMu.Unlock()
}

func (c *ConnMgr) markDisconnectReason(reason string) {
	c.activityMu.Lock()
	c.disconnectReason = reason
	c.activityMu.Unlock()
}

func (c *ConnMgr) takeDisconnectReason(fallback string) string {
	c.activityMu.Lock()
	defer c.activityMu.Unlock()

	if c.disconnectReason != "" {
		reason := c.disconnectReason
		c.disconnectReason = ""
		return reason
	}
	return fallback
}

// Write sends data through the connection and updates activity time
func (c *ConnMgr) Write(data []byte) (int, error) {
	conn := c.GetConn()
	if conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	n, err := conn.Write(data)
	if err == nil && n > 0 {
		// Update last activity time (message sent)
		c.updateLastActivity()
	}
	return n, err
}

func classifyDialError(err error) string {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "dial_timeout"
	}
	return "dial_error"
}

func classifyHandshakeError(err error) string {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "handshake_timeout"
	}
	return "handshake_error"
}

func classifyReadError(err error) string {
	if err == io.EOF {
		return "remote_close"
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "read_timeout"
	}
	return "read_error"
}

func determineActiveChangeReason(oldPriority int, newPriority int) string {
	switch {
	case oldPriority == -1 && newPriority >= 0:
		return "selected"
	case oldPriority >= 0 && newPriority == -1:
		return "connection_lost"
	case oldPriority >= 0 && newPriority > oldPriority:
		return "failover"
	case oldPriority >= 0 && newPriority < oldPriority:
		return "higher_priority_restored"
	default:
		return "active_changed"
	}
}
