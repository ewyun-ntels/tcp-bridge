package tcp

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/connection"
)

// Sender는 TCP 쓰기 작업을 처리합니다.
//
// 특징:
// - 직접 전송(Direct Transmission): 큐 없이 호출 시 즉시 TCP 전송
// - 우선순위 기반 연결: ConnectionManager가 제공하는 READY 연결 사용
// - 프레임 경계 보호: mutex로 frame 단위 쓰기 보장 (중첹 방지)
// - Write Timeout: 설정된 WriteTimeout 적용
//
// 동시성: 여러 goroutine에서 SendFrame() 호출 가능 (mutex로 보호)
type Sender struct {
	logger *slog.Logger
	config *config.TCPConfig

	// Dependencies
	connMgr *connection.ConnectionManager // 우선순위 기반 연결 관리자

	// Concurrency control
	mu sync.Mutex // TCP 쓰기 작업 보호 (프레임 경계 보장)
}

// Reader는 TCP 읽기 작업을 처리합니다.
//
// 아키텍처:
// - ConnectionManager의 콜백 기반 수신: SetFrameReadCallback으로 등록
// - 실제 읽기는 connection.ConnMgr의 readLoop에서 수행
// - Frame 타입에 따른 분기: REQUEST(0x01) / RESPONSE(0x02)
//
// 흐름:
// 1. Start() 호출 → SetFrameReadCallback로 하위 계층에 함수 등록
// 2. TCP 연결에서 frame 수신 → handleFrame() 콜백 호출
// 3. handleFrame에서 타입 분류 → 등록된 콜백 호출
//   - REQUEST: onRequestFrame → TCP→NATS 흐름 (외부 요청)
//   - RESPONSE: onResponseFrame → Inflight-A에서 매칭 처리
//
// 동시성: 실제 읽기는 각 ConnMgr의 readLoop에서 수행 (병렬 처리)
type Reader struct {
	logger  *slog.Logger
	config  *config.TCPConfig
	routing *config.MessageTypeRouting

	// Dependencies
	connMgr *connection.ConnectionManager

	// Callbacks for frame dispatch
	onRequestFrame  func(frame *config.Frame) // TCP→NATS 흐름 처리기
	onResponseFrame func(frame *config.Frame) // NATS→TCP 응답 처리기

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSender creates a new TCP sender
func NewSender(logger *slog.Logger, cfg *config.TCPConfig, connMgr *connection.ConnectionManager) *Sender {
	return &Sender{
		logger:  logger,
		config:  cfg,
		connMgr: connMgr,
	}
}

// NewReader creates a new TCP reader
func NewReader(
	logger *slog.Logger,
	cfg *config.TCPConfig,
	routing *config.MessageTypeRouting,
	connMgr *connection.ConnectionManager,
) *Reader {
	return &Reader{
		logger:  logger,
		config:  cfg,
		routing: routing,
		connMgr: connMgr,
		done:    make(chan struct{}),
	}
}

// SendFrame은 활성 연결을 통해 frame을 직접 전송합니다 (스레드 세이프).
//
// 가장 높은 우선순위의 READY 연결을 사용하며, 사용 불가 시 오류 반환합니다.
//
// 처리 흐름:
// 1. mutex lock으로 frame 경계 보호 (병렬 호출 시 중첹 방지)
// 2. GetActiveConn()으로 READY 상태의 최고 우선순위 연결 획득
// 3. Frame 직렬화 (Type + Length + TID + Payload)
// 4. ConnectionManager.Write()로 전송 (activity time 자동 업데이트)
// 5. 완전한 쓰기 확인 (bytesWritten == len(data))
//
// 오류 처리:
// - 사용 가능한 연결 없음: "no ready connection available"
// - 직렬화 실패: frame 형식 오류
// - 쓰기 실패: 연결 끊김, timeout 등 → ConnMgr이 자동으로 재연결 시도
//
// 동시성: 여러 goroutine에서 안전하게 호출 가능 (mutex로 보호)
func (s *Sender) SendFrame(frame *config.Frame) error {
	// Lock to ensure frame boundary integrity
	s.mu.Lock()
	defer s.mu.Unlock()

	// Serialize frame
	data, err := frame.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize frame: %w", err)
	}

	// Write through ConnectionManager (updates activity time automatically)
	bytesWritten, err := s.connMgr.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}

	if bytesWritten != len(data) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", bytesWritten, len(data))
	}

	// Log header details
	s.logger.Info("TCP frame sent",
		"header", fmt.Sprintf("%02X %02X %02X %02X %02X %02X %02X %02X",
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]),
		"byte1", fmt.Sprintf("0x%02X", data[0]),
		"msg_type", fmt.Sprintf("0x%02X", frame.Type),
		"body_len", uint16(len(data)-8),
		"tid", frame.TID,
		"bytes", bytesWritten)

	return nil
}

// SendFrameToConnection sends a frame to a specific connection by ID.
// Used for sending responses back to the originating connection.
//
// 동시성: 여러 goroutine에서 안전하게 호출 가능 (mutex로 보호)
func (s *Sender) SendFrameToConnection(connectionID string, frame *config.Frame) error {
	// Lock to ensure frame boundary integrity
	s.mu.Lock()
	defer s.mu.Unlock()

	// Serialize frame
	data, err := frame.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize frame: %w", err)
	}

	// Write to specific connection
	bytesWritten, err := s.connMgr.WriteToConnection(connectionID, data)
	if err != nil {
		return fmt.Errorf("failed to write frame to connection %s: %w", connectionID, err)
	}

	if bytesWritten != len(data) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", bytesWritten, len(data))
	}

	// Log header details for specific connection
	s.logger.Info("TCP frame sent to connection",
		"connection_id", connectionID,
		"header", fmt.Sprintf("%02X %02X %02X %02X %02X %02X %02X %02X",
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]),
		"byte1", fmt.Sprintf("0x%02X", data[0]),
		"msg_type", fmt.Sprintf("0x%02X", frame.Type),
		"body_len", uint16(len(data)-8),
		"tid", frame.TID,
		"bytes", bytesWritten)

	return nil
}

// Start starts the TCP reader
func (r *Reader) Start(ctx context.Context) error {
	r.logger.Info("starting TCP reader")

	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx, r.cancel = context.WithCancel(ctx)

	// Set frame read callback on connection manager
	r.connMgr.SetFrameReadCallback(r.handleFrame)

	go r.readerLoop()

	r.logger.Info("TCP reader started")
	return nil
}

// Stop stops the TCP reader
func (r *Reader) Stop() {
	r.logger.Info("stopping TCP reader")

	if r.cancel != nil {
		r.cancel()
	}

	// Wait for reader loop to exit
	select {
	case <-r.done:
		r.logger.Info("TCP reader stopped")
	case <-time.After(5 * time.Second):
		r.logger.Warn("TCP reader stop timeout")
	}
}

// SetRequestFrameCallback sets the callback for request frames
func (r *Reader) SetRequestFrameCallback(callback func(frame *config.Frame)) {
	r.onRequestFrame = callback
}

// SetResponseFrameCallback sets the callback for response frames
func (r *Reader) SetResponseFrameCallback(callback func(frame *config.Frame)) {
	r.onResponseFrame = callback
}

// readerLoop manages the reader lifecycle
func (r *Reader) readerLoop() {
	defer close(r.done)

	// Reader lifecycle is managed by connection manager
	// This loop just waits for context cancellation
	<-r.ctx.Done()
}

// handleFrame processes incoming frames and dispatches by frame type
func (r *Reader) handleFrame(frame *config.Frame) {
	r.logger.Debug("frame received",
		"tid", frame.TID,
		"type", fmt.Sprintf("0x%02x", frame.Type),
		"payload_size", len(frame.Payload))

	// Handshake frames are handled by connection manager (4.1.1.d: 0x01, 0x02)
	if frame.IsHandshake() {
		r.logger.Debug("handshake frame ignored in reader", "type", fmt.Sprintf("0x%02x", frame.Type))
		return
	}

	// Ping/Pong frames are handled by connection manager (4.1.1.d: 0x03, 0x04)
	if frame.IsPing() {
		r.logger.Debug("ping/pong frame ignored in reader", "type", fmt.Sprintf("0x%02x", frame.Type))
		return
	}

	// Business messages are classified by configured message_type_routing.
	if r.routing.IsInboundRequestType(frame.Type) {
		// TCP inbound request -> spawn goroutine for TCP→NATS processing
		if r.onRequestFrame != nil {
			r.onRequestFrame(frame)
		} else {
			r.logger.Warn("no handler for request frame", "tid", frame.TID, "type", fmt.Sprintf("0x%02x", frame.Type))
		}
	} else if r.routing.IsResponseType(frame.Type) {
		// TCP response -> match with inflight-A entries (NATS→TCP)
		if r.onResponseFrame != nil {
			r.onResponseFrame(frame)
		} else {
			r.logger.Warn("no handler for response frame", "tid", frame.TID, "type", fmt.Sprintf("0x%02x", frame.Type))
		}
	} else {
		r.logger.Warn("unknown frame type", "type", fmt.Sprintf("0x%02x", frame.Type), "tid", frame.TID)
	}
}
