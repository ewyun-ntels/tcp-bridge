package inflight

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tcp-bridge/internal/config"
)

// InflightManager는 양방향 진행 중인 요청을 관리합니다.
//
// 두 가지 독립적인 Inflight 관리자를 통합 관리:
// - Inflight-A: NATS에서 TCP로 전송된 RPC 요청 추적 (External → TCP Server)
// - Inflight-B: TCP에서 NATS로 전송된 RPC 요청 추적 (TCP Server → External)
//
// 각 Inflight는 독립적으로 작동하며, TID를 키로 사용하여 요청-응답을 매칭합니다.
// 만료된 요청은 주기적인 cleanup 루프에서 자동으로 제거됩니다.
type InflightManager struct {
	logger *slog.Logger

	// Inflight-A: NATS→TCP (RPC only)
	inflightA *InflightA

	// Inflight-B: TCP→NATS (ALWAYS for TCP inbound RPC)
	inflightB *InflightB
}

// InflightA는 NATS→TCP 방향의 대기 중인 요청을 관리합니다.
//
// 처리 흐름:
// 1. NATS에서 요청 수신 → Register() 호출하여 TID로 등록
// 2. TCP로 전송 후 ResponseChan에서 응답 대기
// 3. TCP 응답 수신 → HandleResponse()로 매칭된 entry에 응답 전달
// 4. Deadline 초과 시 cleanup 루프에서 자동 제거 및 timeout 처리
//
// 동시성: mutex로 보호되며 여러 goroutine에서 안전하게 사용 가능
type InflightA struct {
	logger *slog.Logger

	mu      sync.RWMutex
	entries map[uint32]*InflightEntryA // TID → Entry 매핑

	// Cleanup
	cleanupInterval time.Duration      // 만료 검사 주기 (기본 30초)
	ctx             context.Context    // cleanup 루프 제어용
	cancel          context.CancelFunc // cleanup 루프 종료용
	done            chan struct{}      // cleanup 루프 완료 신호
}

// InflightB manages TCP→NATS pending requests (ALWAYS for TCP inbound)
type InflightB struct {
	logger *slog.Logger

	mu       sync.RWMutex
	entries  map[string]*InflightEntryB // key: "connectionID:TID"
	onExpire func(entry *InflightEntryB)

	// Cleanup
	cleanupInterval time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
}

// InflightEntryA represents a NATS→TCP inflight entry
type InflightEntryA struct {
	TID                  uint32 // Transaction ID
	ReplySubject         string // NATS reply subject (inbox)
	Subject              string // NATS request subject
	RequestType          uint8  // TCP request type
	ExpectedResponseType uint8  // Expected TCP response type
	Deadline             int64  // Request deadline (unix timestamp)
	ConnectionID         string // 요청을 실제로 전송한 TCP connection

	// Response handling
	ResponseChan chan *config.Frame // Channel to receive response
}

// InflightEntryB represents a TCP→NATS inflight entry.
//
// 현재 inbound 경로는 NATS request/reply를 동기 호출로 처리하므로
// 이 구조체는 원래 TCP connection 기준 lifecycle 추적용 메타데이터만 보관합니다.
type InflightEntryB struct {
	TID          uint32        // Transaction ID
	RequestType  uint8         // Original inbound TCP request type
	TCPReplyInfo *TCPReplyInfo // TCP reply information
	Payload      []byte        // Request payload
	Deadline     int64         // Request deadline (unix timestamp)
}

// TCPReplyInfo contains information needed to reply via TCP
type TCPReplyInfo struct {
	ConnectionID string // Connection identifier
	FrameType    uint8  // Response frame type
}

// NewInflightManager creates a new inflight manager
func NewInflightManager(logger *slog.Logger) *InflightManager {
	return &InflightManager{
		logger:    logger,
		inflightA: NewInflightA(logger.With("component", "inflight-natsinbound")),
		inflightB: NewInflightB(logger.With("component", "inflight-tcpinbound")),
	}
}

// NewInflightA creates a new Inflight-A manager
func NewInflightA(logger *slog.Logger) *InflightA {
	return &InflightA{
		logger:          logger,
		entries:         make(map[uint32]*InflightEntryA),
		cleanupInterval: 30 * time.Second,
		done:            make(chan struct{}),
	}
}

// NewInflightB creates a new Inflight-B manager
func NewInflightB(logger *slog.Logger) *InflightB {
	return &InflightB{
		logger:          logger,
		entries:         make(map[string]*InflightEntryB),
		cleanupInterval: 30 * time.Second,
		done:            make(chan struct{}),
	}
}

// Start starts the inflight manager
func (im *InflightManager) Start(ctx context.Context) error {
	im.logger.Info("starting inflight manager")

	if err := im.inflightA.Start(ctx); err != nil {
		return fmt.Errorf("failed to start inflight-a: %w", err)
	}

	if err := im.inflightB.Start(ctx); err != nil {
		im.inflightA.Stop()
		return fmt.Errorf("failed to start inflight-b: %w", err)
	}

	im.logger.Info("inflight manager started")
	return nil
}

// Stop stops the inflight manager
func (im *InflightManager) Stop() {
	im.logger.Info("stopping inflight manager")

	im.inflightA.Stop()
	im.inflightB.Stop()

	im.logger.Info("inflight manager stopped")
}

// GetInflightA returns the Inflight-A manager
func (im *InflightManager) GetInflightA() *InflightA {
	return im.inflightA
}

// GetInflightB returns the Inflight-B manager
func (im *InflightManager) GetInflightB() *InflightB {
	return im.inflightB
}

// Start starts the Inflight-A manager
func (ia *InflightA) Start(ctx context.Context) error {
	ia.logger.Info("starting inflight-a")

	if ctx == nil {
		ctx = context.Background()
	}
	ia.ctx, ia.cancel = context.WithCancel(ctx)

	go ia.cleanupLoop()

	ia.logger.Info("inflight-a started")
	return nil
}

// Stop stops the Inflight-A manager
func (ia *InflightA) Stop() {
	ia.logger.Info("stopping inflight-a")

	if ia.cancel != nil {
		ia.cancel()
	}

	// Clean up all entries
	ia.mu.Lock()
	for tid, entry := range ia.entries {
		close(entry.ResponseChan)
		delete(ia.entries, tid)
	}
	ia.mu.Unlock()

	// Wait for cleanup loop to exit
	select {
	case <-ia.done:
		ia.logger.Info("inflight-a stopped")
	case <-time.After(5 * time.Second):
		ia.logger.Warn("inflight-a stop timeout")
	}
}

// Register는 새로운 NATS→TCP 요청을 등록합니다.
//
// 파라미터:
//   - tid: 고유 Transaction ID (globalTIDCounter에서 생성)
//   - replySubject: NATS inbox 주소 (응답 전송용)
//   - deadline: 요청 만료 시각 (Unix timestamp)
//
// 반환값: 생성된 InflightEntryA (ResponseChan에서 응답 대기용)
//
// 주의: 반환된 entry의 ResponseChan은 반드시 select로 수신해야 합니다.
func (ia *InflightA) Register(
	tid uint32,
	replySubject string,
	subject string,
	requestType uint8,
	expectedResponseType uint8,
	deadline int64,
) *InflightEntryA {
	entry := &InflightEntryA{
		TID:                  tid,
		ReplySubject:         replySubject,
		Subject:              subject,
		RequestType:          requestType,
		ExpectedResponseType: expectedResponseType,
		Deadline:             deadline,
		ResponseChan:         make(chan *config.Frame, 1),
	}

	ia.mu.Lock()
	ia.entries[tid] = entry
	ia.mu.Unlock()

	ia.logger.Debug("registered inflight-a entry",
		"tid", tid,
		"reply_subject", replySubject,
		"subject", subject,
		"request_type", fmt.Sprintf("0x%02x", requestType),
		"expected_response_type", fmt.Sprintf("0x%02x", expectedResponseType))

	return entry
}

// HandleResponse는 TCP에서 수신한 응답을 처리합니다.
//
// 처리 흐름:
// 1. frame.TID로 대기 중인 entry 검색 및 제거 (즉시 삭제로 중복 처리 방지)
// 2. entry의 ResponseChan에 응답 Frame 전달
// 3. 대기 중인 goroutine이 응답을 받아 NATS reply 전송
//
// 반환값:
//   - true: 매칭된 entry를 찾아 응답 전달 성공
//   - false: 해당 TID의 entry가 없음 (이미 timeout되었거나 잘못된 TID)
//
// 동시성: mutex로 보호되며, 여러 TCP reader에서 동시 호출 가능
func (ia *InflightA) HandleResponse(frame *config.Frame) bool {
	ia.mu.Lock()
	entry, exists := ia.entries[frame.TID]
	if exists && entry.ConnectionID != "" && entry.ConnectionID != frame.ConnectionID {
		ia.mu.Unlock()
		ia.logger.Warn("inflight-a response connection mismatch",
			"tid", frame.TID,
			"expected_connection_id", entry.ConnectionID,
			"actual_connection_id", frame.ConnectionID)
		return false
	}
	if exists && entry.ExpectedResponseType != 0 && entry.ExpectedResponseType != frame.Type {
		ia.mu.Unlock()
		ia.logger.Warn("inflight-a response type mismatch",
			"tid", frame.TID,
			"subject", entry.Subject,
			"request_type", fmt.Sprintf("0x%02x", entry.RequestType),
			"expected_response_type", fmt.Sprintf("0x%02x", entry.ExpectedResponseType),
			"actual_response_type", fmt.Sprintf("0x%02x", frame.Type))
		return false
	}
	if exists {
		delete(ia.entries, frame.TID) // 즉시 삭제 → 중복 처리 방지
	}
	ia.mu.Unlock()

	if !exists {
		ia.logger.Warn("no inflight-a entry found for response",
			"tid", frame.TID,
			"connection_id", frame.ConnectionID,
			"response_type", fmt.Sprintf("0x%02x", frame.Type),
			"payload_size", len(frame.Payload))
		return false
	}

	// 대기 중인 goroutine에 응답 전달
	select {
	case entry.ResponseChan <- frame:
		ia.logger.Debug("response delivered to inflight-a entry", "tid", frame.TID)
	default:
		// 버퍼 크기 1이므로 거의 발생하지 않음 (이미 timeout된 경우)
		ia.logger.Warn("response channel full for inflight-a entry", "tid", frame.TID)
	}

	return true
}

// SetConnectionID records which TCP connection was used for a NATS→TCP request.
func (ia *InflightA) SetConnectionID(tid uint32, connectionID string) bool {
	ia.mu.Lock()
	defer ia.mu.Unlock()

	entry, exists := ia.entries[tid]
	if !exists {
		return false
	}
	entry.ConnectionID = connectionID
	return true
}

// Remove removes an inflight entry
func (ia *InflightA) Remove(tid uint32) {
	ia.mu.Lock()
	delete(ia.entries, tid)
	ia.mu.Unlock()

	ia.logger.Debug("removed inflight-a entry", "tid", tid)
}

// cleanupLoop periodically cleans up expired entries
func (ia *InflightA) cleanupLoop() {
	defer close(ia.done)

	ticker := time.NewTicker(ia.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ia.ctx.Done():
			return

		case <-ticker.C:
			ia.cleanupExpired()
		}
	}
}

// cleanupExpired removes expired entries
func (ia *InflightA) cleanupExpired() {
	now := time.Now().Unix()
	var expiredTIDs []uint32

	ia.mu.RLock()
	for tid, entry := range ia.entries {
		if entry.Deadline < now {
			expiredTIDs = append(expiredTIDs, tid)
		}
	}
	ia.mu.RUnlock()

	if len(expiredTIDs) > 0 {
		ia.mu.Lock()
		for _, tid := range expiredTIDs {
			delete(ia.entries, tid)
		}
		ia.mu.Unlock()

		ia.logger.Info("cleaned up expired inflight-a entries", "count", len(expiredTIDs))
	}
}

// Start starts the Inflight-B manager
func (ib *InflightB) Start(ctx context.Context) error {
	ib.logger.Info("starting inflight-b")

	if ctx == nil {
		ctx = context.Background()
	}
	ib.ctx, ib.cancel = context.WithCancel(ctx)

	go ib.cleanupLoop()

	ib.logger.Info("inflight-b started")
	return nil
}

// Stop stops the Inflight-B manager
func (ib *InflightB) Stop() {
	ib.logger.Info("stopping inflight-b")

	if ib.cancel != nil {
		ib.cancel()
	}

	// Clean up all entries
	ib.mu.Lock()
	for key := range ib.entries {
		delete(ib.entries, key)
	}
	ib.mu.Unlock()

	// Wait for cleanup loop to exit
	select {
	case <-ib.done:
		ib.logger.Info("inflight-b stopped")
	case <-time.After(5 * time.Second):
		ib.logger.Warn("inflight-b stop timeout")
	}
}

// Register registers a new TCP→NATS request (ALWAYS for TCP inbound)
func (ib *InflightB) Register(tid uint32, requestType uint8, tcpReplyInfo *TCPReplyInfo, payload []byte, deadline int64) *InflightEntryB {
	entry := &InflightEntryB{
		TID:          tid,
		RequestType:  requestType,
		TCPReplyInfo: tcpReplyInfo,
		Payload:      payload,
		Deadline:     deadline,
	}

	key := fmt.Sprintf("%s:%d", tcpReplyInfo.ConnectionID, tid)
	ib.mu.Lock()
	ib.entries[key] = entry
	ib.mu.Unlock()

	ib.logger.Debug("registered inflight-b entry",
		"tid", tid,
		"connection_id", tcpReplyInfo.ConnectionID,
		"key", key)

	return entry
}

func (ib *InflightB) SetExpireCallback(callback func(entry *InflightEntryB)) {
	ib.onExpire = callback
}

// Remove removes an inflight entry
func (ib *InflightB) Remove(connectionID string, tid uint32) {
	key := fmt.Sprintf("%s:%d", connectionID, tid)

	ib.mu.Lock()
	delete(ib.entries, key)
	ib.mu.Unlock()

	ib.logger.Debug("removed inflight-b entry",
		"connection_id", connectionID,
		"tid", tid,
		"key", key)
}

// cleanupLoop periodically cleans up expired entries
func (ib *InflightB) cleanupLoop() {
	defer close(ib.done)

	ticker := time.NewTicker(ib.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ib.ctx.Done():
			return

		case <-ticker.C:
			ib.cleanupExpired()
		}
	}
}

// cleanupExpired removes expired entries
func (ib *InflightB) cleanupExpired() {
	now := time.Now().Unix()
	var expiredEntries []*InflightEntryB
	var expiredKeys []string

	ib.mu.RLock()
	for key, entry := range ib.entries {
		if entry.Deadline < now {
			expiredKeys = append(expiredKeys, key)
			expiredEntries = append(expiredEntries, entry)
		}
	}
	ib.mu.RUnlock()

	if len(expiredKeys) > 0 {
		ib.mu.Lock()
		for _, key := range expiredKeys {
			delete(ib.entries, key)
		}
		ib.mu.Unlock()

		ib.logger.Info("cleaned up expired inflight-b entries", "count", len(expiredKeys))
		if ib.onExpire != nil {
			for _, entry := range expiredEntries {
				ib.onExpire(entry)
			}
		}
	}
}
