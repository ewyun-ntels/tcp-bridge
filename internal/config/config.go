package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// Config는 애플리케이션 전체 설정을 나타냅니다.
//
// YAML 파일에서 로드되며, 각 섹션은 다음을 정의합니다:
// - server: 서버 메타데이터 (이름, 버전, 환경, shutdown timeout)
// - tcp: TCP 연결 설정 (다중 endpoint, timeout, frame 크기 등)
// - nats: NATS 설정 (URL, subject, routing, timeout)
// - message_handler: NATS↔TCP 메시지 처리 설정 (timeout, retry)
// - queue: 전송 큐 설정 (현재 미사용, 향후 확장 대비)
// - metrics: Prometheus 메트릭 설정 (HTTP 포트, 경로, 기본 runtime 메트릭 포함 여부)
// - logging: 로그 레벨 설정 (debug/info/warn/error)
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	TCP            TCPConfig            `yaml:"tcp"`
	NATS           NATSConfig           `yaml:"nats"`
	MessageHandler MessageHandlerConfig `yaml:"message_handler"`
	Queue          QueueConfig          `yaml:"queue"`
	Metrics        MetricsConfig        `yaml:"metrics"`
	Logging        LoggingConfig        `yaml:"logging"`
	Alerta         AlertaConfig         `yaml:"alerta"`
}

// ServerConfig contains general server configuration
type ServerConfig struct {
	Name            string        `yaml:"name"`
	Version         string        `yaml:"version"`
	Environment     string        `yaml:"environment"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// TCPConfig는 TCP 관련 설정을 포함합니다.
//
// 우선순위 기반 다중 연결:
// - Endpoints: priority 값으로 정렬됨 (0=Primary, 1=Secondary, ...)
// - 연결 실패 시 자동으로 다음 우선순위 endpoint로 재연결 시도
// - READY 상태의 가장 높은 우선순위 연결을 활성 연결으로 사용
//
// Frame 프로토콜 (4.1):
// - Header: 8 bytes
//   - Byte 1: Extension Bit(1) + Protocol Version(2) + Reserved(5)
//   - Byte 2: Message Type
//   - Byte 3-4: Body Length (Big Endian, uint16)
//   - Byte 5-8: Transaction Identifier (Big Endian, uint32)
//
// - MaxFrameSize: 프레임 payload 최대 크기 (NATS max_payload: 1MB)
//
// Handshake:
// - 연결 후 HELLO(0x01) 전송 → ACK(0x02) 대기
// - HandshakeTimeout 내 ACK 미수신 시 연결 종료
//
// Ping/Pong:
// - idle 시 PING(0x03) 전송 → PONG(0x04) 대기
// - PingTimeout 내 PONG 미수신 시 연결 종료 및 재연결
type TCPConfig struct {
	Endpoints []TCPEndpoint `yaml:"endpoints"` // 외부 서버 endpoint 목록 (priority 기반 정렬)

	// Connection settings
	ConnectTimeout time.Duration `yaml:"connect_timeout"` // 연결 시도 timeout
	ReadTimeout    time.Duration `yaml:"read_timeout"`    // (미사용) ping-pong으로 연결 상태 관리
	WriteTimeout   time.Duration `yaml:"write_timeout"`   // 단일 write 작업 timeout
	KeepAlive      time.Duration `yaml:"keep_alive"`      // TCP KeepAlive interval

	// Frame settings
	MaxFrameSize    int `yaml:"max_frame_size"`    // 최대 frame payload 크기 (프로토콜 스펙: uint16 최대값 65535)
	FrameHeaderSize int `yaml:"frame_header_size"` // Frame header 크기 (8 bytes)

	// Hello/ACK handshake
	HandshakeTimeout time.Duration `yaml:"handshake_timeout"` // HELLO/ACK handshake timeout
	SysPrefixID      string        `yaml:"sys_prefix_id"`      // Peer Name Prefix (POD_INDEX 또는 Pod 이름에서 추출한 index가 추가되어 최종 SysID 생성)
	BranchName       string        `yaml:"branch_name"`       // 국사명 (SS/DS/BR)

	// Ping/Pong
	PingTimeout        time.Duration `yaml:"ping_timeout"`         // PING 전송 후 PONG 대기 timeout
	PingIntervalMargin time.Duration `yaml:"ping_interval_margin"` // HELLO ACK의 ping-interval에서 차감할 안전 마진
}

// TCPEndpoint represents a TCP connection endpoint
type TCPEndpoint struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Priority int    `yaml:"priority"`
}

// NATSConfig contains NATS-related configuration
type NATSConfig struct {
	URLs               []string           `yaml:"urls"`
	ConnectTimeout     time.Duration      `yaml:"connect_timeout"`
	MessageTypeRouting MessageTypeRouting `yaml:"message_type_routing"` // TCP→NATS Message Type별 subject 매핑
}

// MessageTypeRouting defines how to route TCP messages to NATS subjects based on Message Type
type MessageTypeRouting struct {
	Outbound map[string]OutboundRoute `yaml:"outbound"` // NATS → TCP 라우팅
	Inbound  map[string]InboundRoute  `yaml:"inbound"`  // TCP → NATS 라우팅
}

// OutboundRoute defines routing info for the outbound direction (NATS → TCP).
type OutboundRoute struct {
	Subject         string `yaml:"subject"`           // NATS subject
	ResponseMsgType string `yaml:"response_msg_type"` // 기대되는 응답 메시지 타입 (hex)
}

// InboundRoute defines routing info for the inbound direction (TCP → NATS).
type InboundRoute struct {
	Subject         string `yaml:"subject"`           // NATS subject
	ResponseMsgType string `yaml:"response_msg_type"` // TCP로 보낼 응답 메시지 타입 (hex)
}

// GetInboundSubjectForMessageType returns the NATS subject for a TCP → NATS request type.
// Returns empty string if no mapping exists
func (m *MessageTypeRouting) GetInboundSubjectForMessageType(msgType uint8) string {
	key := fmt.Sprintf("%02x", msgType)
	if route, exists := m.Inbound[key]; exists {
		return route.Subject
	}
	return ""
}

// GetOutboundRouteBySubject returns the NATS→TCP route for a given subject.
func (m *MessageTypeRouting) GetOutboundRouteBySubject(subject string) (OutboundRoute, bool) {
	for _, route := range m.Outbound {
		if route.Subject == subject {
			return route, true
		}
	}
	return OutboundRoute{}, false
}

// GetOutboundSubjects returns all configured NATS→TCP subjects.
func (m *MessageTypeRouting) GetOutboundSubjects() []string {
	subjects := make([]string, 0, len(m.Outbound))
	for _, route := range m.Outbound {
		if route.Subject != "" {
			subjects = append(subjects, route.Subject)
		}
	}
	return subjects
}

// IsRequestType returns true if msgType is configured as a request type in either direction.
func (m *MessageTypeRouting) IsRequestType(msgType uint8) bool {
	key := fmt.Sprintf("%02x", msgType)
	if _, exists := m.Outbound[key]; exists {
		return true
	}
	if _, exists := m.Inbound[key]; exists {
		return true
	}
	return false
}

// IsResponseType returns true if msgType is configured as a response type in either direction.
func (m *MessageTypeRouting) IsResponseType(msgType uint8) bool {
	key := fmt.Sprintf("%02x", msgType)

	for _, route := range m.Outbound {
		if route.ResponseMsgType == key {
			return true
		}
	}
	for _, route := range m.Inbound {
		if route.ResponseMsgType == key {
			return true
		}
	}
	return false
}

// IsOutboundRequestType returns true if msgType is configured for NATS→TCP requests.
func (m *MessageTypeRouting) IsOutboundRequestType(msgType uint8) bool {
	key := fmt.Sprintf("%02x", msgType)
	_, exists := m.Outbound[key]
	return exists
}

// IsInboundRequestType returns true if msgType is configured for TCP→NATS requests.
func (m *MessageTypeRouting) IsInboundRequestType(msgType uint8) bool {
	key := fmt.Sprintf("%02x", msgType)
	_, exists := m.Inbound[key]
	return exists
}

// GetResponseType returns the response message type for given request type
// Returns 0 if no mapping exists
func (m *MessageTypeRouting) GetResponseType(msgType uint8) uint8 {
	key := fmt.Sprintf("%02x", msgType)

	// TCP → NATS 방향 확인
	if route, exists := m.Inbound[key]; exists {
		var respType uint8
		fmt.Sscanf(route.ResponseMsgType, "%02x", &respType)
		return respType
	}

	// NATS → TCP 방향 확인
	if route, exists := m.Outbound[key]; exists {
		var respType uint8
		fmt.Sscanf(route.ResponseMsgType, "%02x", &respType)
		return respType
	}

	return 0
}

// MessageHandlerConfig contains message handler configuration
type MessageHandlerConfig struct {
	Outbound MessageHandlerPoolConfig `yaml:"outbound"` // NATS→TCP processing
	Inbound  MessageHandlerPoolConfig `yaml:"inbound"`  // TCP→NATS processing
}

// MessageHandlerPoolConfig represents configuration for message handling
type MessageHandlerPoolConfig struct {
	Timeout         time.Duration `yaml:"timeout"`          // 전체 처리 timeout (optional, 안전장치)
	ResponseTimeout time.Duration `yaml:"response_timeout"` // 각 시도별 응답 대기 시간
	RetryAttempts   int           `yaml:"retry_attempts"`   // 같은 connection 재시도 횟수
	WorkerCount     int           `yaml:"worker_count"`     // 처리 worker 수
	QueueSize       int           `yaml:"queue_size"`       // 처리 큐 크기
}

// QueueConfig contains queue configuration
type QueueConfig struct {
	SendQueueSize int           `yaml:"send_queue_size"`
	SendTimeout   time.Duration `yaml:"send_timeout"`
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Enabled               bool   `yaml:"enabled"`
	Port                  int    `yaml:"port"`
	Path                  string `yaml:"path"`
	IncludeDefaultMetrics bool   `yaml:"include_default_metrics"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// AlertaConfig contains Alerta alarm system configuration
// 2026-03-27: 추가 — service는 sysID, tags는 event/group에서 자동 생성
type AlertaConfig struct {
	Enabled     bool          `yaml:"enabled"`
	URL         string        `yaml:"url"`
	Timeout     time.Duration `yaml:"timeout"`
	Environment string        `yaml:"environment"`
	Alerts      []AlertDef    `yaml:"alerts"`
}

// AlertDef defines an individual alert type
type AlertDef struct {
	Event   string `yaml:"event"`
	Group   string `yaml:"group"`
	ErrCode string `yaml:"err_code"`
}

// Address returns the formatted address for TCP endpoint
func (e TCPEndpoint) Address() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

// LoadConfig loads configuration from file
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	setDefaults(&cfg)

	// Validate
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate checks the configuration for invalid or missing values.
// Called after setDefaults, so only checks values that must be explicitly provided
// or values that could be set to invalid ranges.
func (cfg *Config) Validate() error {
	var errs []string

	// TCP endpoints
	if len(cfg.TCP.Endpoints) == 0 {
		errs = append(errs, "at least one TCP endpoint is required")
	}
	for i, ep := range cfg.TCP.Endpoints {
		if ep.Host == "" {
			errs = append(errs, fmt.Sprintf("tcp.endpoints[%d]: host is required", i))
		}
		if ep.Port <= 0 || ep.Port > 65535 {
			errs = append(errs, fmt.Sprintf("tcp.endpoints[%d]: invalid port %d (must be 1-65535)", i, ep.Port))
		}
	}

	// NATS URLs
	if len(cfg.NATS.URLs) == 0 {
		errs = append(errs, "at least one NATS URL is required")
	}

	// Message handler pools
	if cfg.MessageHandler.Outbound.WorkerCount <= 0 {
		errs = append(errs, "message_handler.outbound.worker_count must be > 0")
	}
	if cfg.MessageHandler.Inbound.WorkerCount <= 0 {
		errs = append(errs, "message_handler.inbound.worker_count must be > 0")
	}
	if cfg.MessageHandler.Outbound.ResponseTimeout <= 0 {
		errs = append(errs, "message_handler.outbound.response_timeout must be > 0")
	}
	if cfg.MessageHandler.Inbound.ResponseTimeout <= 0 {
		errs = append(errs, "message_handler.inbound.response_timeout must be > 0")
	}

	// Metrics port
	if cfg.Metrics.Enabled && (cfg.Metrics.Port <= 0 || cfg.Metrics.Port > 65535) {
		errs = append(errs, fmt.Sprintf("metrics.port: invalid port %d (must be 1-65535)", cfg.Metrics.Port))
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// setDefaults sets default values for configuration
func setDefaults(cfg *Config) {
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = 30 * time.Second
	}

	if cfg.TCP.ConnectTimeout == 0 {
		cfg.TCP.ConnectTimeout = 5 * time.Second
	}

	// ReadTimeout: 미사용 (ping-pong으로 연결 상태 관리)
	if cfg.TCP.ReadTimeout == 0 {
		cfg.TCP.ReadTimeout = 30 * time.Second
	}

	if cfg.TCP.WriteTimeout == 0 {
		cfg.TCP.WriteTimeout = 30 * time.Second
	}

	if cfg.TCP.KeepAlive == 0 {
		cfg.TCP.KeepAlive = 30 * time.Second
	}

	if cfg.TCP.MaxFrameSize == 0 {
		cfg.TCP.MaxFrameSize = 65535 // 64KB (프로토콜 스펙: Body Length는 uint16 최대값)
	}

	if cfg.TCP.FrameHeaderSize == 0 {
		cfg.TCP.FrameHeaderSize = 8 // 4 bytes length + 4 bytes tid
	}

	if cfg.TCP.HandshakeTimeout == 0 {
		cfg.TCP.HandshakeTimeout = 10 * time.Second
	}

	if cfg.TCP.PingTimeout == 0 {
		cfg.TCP.PingTimeout = 5 * time.Second
	}

	if cfg.NATS.ConnectTimeout == 0 {
		cfg.NATS.ConnectTimeout = 5 * time.Second
	}

	// Set default message type routing if not configured
	if cfg.NATS.MessageTypeRouting.Outbound == nil {
		cfg.NATS.MessageTypeRouting.Outbound = map[string]OutboundRoute{
			"05": {Subject: "tcp.subs.change", ResponseMsgType: "06"},
			"09": {Subject: "tcp.cellinfo.noti", ResponseMsgType: "0a"},
		}
	}
	if cfg.NATS.MessageTypeRouting.Inbound == nil {
		cfg.NATS.MessageTypeRouting.Inbound = map[string]InboundRoute{
			"07": {Subject: "tcp.subs.info", ResponseMsgType: "08"}, // Subs-Info
			"0b": {Subject: "tcp.subs.sync", ResponseMsgType: "0c"}, // Subs-Sync
		}
	}

	if cfg.MessageHandler.Outbound.Timeout == 0 {
		cfg.MessageHandler.Outbound.Timeout = 60 * time.Second
	}

	if cfg.MessageHandler.Inbound.Timeout == 0 {
		cfg.MessageHandler.Inbound.Timeout = 60 * time.Second
	}

	if cfg.MessageHandler.Outbound.ResponseTimeout == 0 {
		cfg.MessageHandler.Outbound.ResponseTimeout = 5 * time.Second
	}

	if cfg.MessageHandler.Inbound.ResponseTimeout == 0 {
		cfg.MessageHandler.Inbound.ResponseTimeout = 5 * time.Second
	}

	if cfg.MessageHandler.Outbound.RetryAttempts == 0 {
		cfg.MessageHandler.Outbound.RetryAttempts = 3
	}

	if cfg.MessageHandler.Inbound.RetryAttempts == 0 {
		cfg.MessageHandler.Inbound.RetryAttempts = 3
	}

	if cfg.MessageHandler.Outbound.WorkerCount == 0 {
		cfg.MessageHandler.Outbound.WorkerCount = 4
	}

	if cfg.MessageHandler.Inbound.WorkerCount == 0 {
		cfg.MessageHandler.Inbound.WorkerCount = 4
	}

	if cfg.MessageHandler.Outbound.QueueSize == 0 {
		cfg.MessageHandler.Outbound.QueueSize = 128
	}

	if cfg.MessageHandler.Inbound.QueueSize == 0 {
		cfg.MessageHandler.Inbound.QueueSize = 128
	}

	if cfg.Queue.SendQueueSize == 0 {
		cfg.Queue.SendQueueSize = 10000
	}

	if cfg.Queue.SendTimeout == 0 {
		cfg.Queue.SendTimeout = 5 * time.Second
	}

	if cfg.Metrics.Port == 0 {
		cfg.Metrics.Port = 8080
	}

	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}

	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}

	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "json"
	}

	// Alerta defaults
	if cfg.Alerta.Timeout == 0 {
		cfg.Alerta.Timeout = 3 * time.Second
	}
	if cfg.Alerta.Environment == "" {
		cfg.Alerta.Environment = "Development"
	}
}
