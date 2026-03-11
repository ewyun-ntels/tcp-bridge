package nats

import (
	"fmt"
	"log/slog"
	"time"

	"tcp-bridge/internal/config"

	"github.com/nats-io/nats.go"
)

// Client는 NATS 연결 및 작업을 관리합니다.
//
// 아키텍처:
// - Subscriber: Core NATS Queue Group 기반 구독 (외부→TCP 흐름)
// - ReplyPublisher: NATS reply 발행 (외부→TCP 응답, inbox reply)
// - Requester: NATS Request-Reply (TCP→외부 흐름)
//
// 동시성:
// - 모든 컴포넨트는 동일한 nats.Conn 공유
// - nats.Conn은 thread-safe하므로 병렬 호출 가능
//
// 재연결:
// - NATS 라이브러리가 자동 재연결 처리 (MaxReconnects=-1 무한)
// - Disconnect/Reconnect/Closed 핸들러 등록되어 상태 모니터링
type Client struct {
	logger *slog.Logger
	config *config.NATSConfig

	// NATS connection
	conn *nats.Conn // 공유 연결 (thread-safe)

	// Components
	subscriber     *Subscriber     // Queue Group 구독자 (NATS→TCP subjects)
	replyPublisher *ReplyPublisher // Reply 발행기 (inbox로 reply)
	requester      *Requester      // Request-Reply 클라이언트
}

// Subscriber는 NATS 구독을 처리합니다 (A: NATS→TCP subjects).
//
// Queue Group 패턴:
// - 동일 subject에 대해 여러 tcp-bridge 인스턴스가 queue group으로 구독
// - 각 메시지는 한 인스턴스만 받음 (부하 분산)
// - Core NATS이므로 별도 ACK/NAK 개념은 사용하지 않음
//
// 처리 흐름:
// 1. NATS에서 메시지 수신 → onMessage 콜백 호출
// 2. BridgeHandler.HandleOutboundRequest에서 TCP 전송
// 3. TCP 응답 수신 후 msg.Reply로 응답 전송
// 4. 처리 결과에 따라 reply를 발행하거나 로그를 남김
type Subscriber struct {
	logger *slog.Logger
	config *config.NATSConfig
	conn   *nats.Conn

	// Subscription
	subs []*nats.Subscription // Core NATS Queue Group 구독

	// Callback for received messages
	onMessage func(msg *nats.Msg) // 메시지 수신 시 호출되는 콜백
}

// ReplyPublisher는 NATS reply 발행을 처리합니다 (A: responses to NATS).
//
// 응답 패턴:
// - 외부에서 받은 msg.Reply에 응답 발행
// - inbox 주소로 직접 Publish (일반 Pub/Sub)
// - ACK/NAK 패턴과 별개로 애플리케이션 수준 응답 전송
type ReplyPublisher struct {
	logger *slog.Logger
	config *config.NATSConfig
	conn   *nats.Conn
}

// Requester는 NATS 요청을 처리합니다.
//
// Request-Reply 패턴:
// - TCP에서 받은 REQUEST frame을 NATS Request로 전환
// - conn.Request(subject, data, timeout)으로 RPC 호출
// - 외부 서비스에서 응답 수신 후 TCP RESPONSE frame으로 전송
type Requester struct {
	logger *slog.Logger
	config *config.NATSConfig
	conn   *nats.Conn
}

// NewClient creates a new NATS client
func NewClient(logger *slog.Logger, cfg *config.NATSConfig) *Client {
	return &Client{
		logger: logger,
		config: cfg,
	}
}

// Connect connects to NATS server
func (c *Client) Connect() error {
	c.logger.Info("connecting to NATS", "urls", c.config.URLs)

	// Configure connection options
	opts := []nats.Option{
		nats.Name("tcp-bridge"),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1), // Unlimited reconnects
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				c.logger.Error("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			c.logger.Info("NATS connection closed", "error", nc.LastError())
		}),
	}

	// Connect to NATS
	urlsStr := ""
	if len(c.config.URLs) > 0 {
		urlsStr = c.config.URLs[0]
		if len(c.config.URLs) > 1 {
			// Join multiple URLs with comma
			for i := 1; i < len(c.config.URLs); i++ {
				urlsStr += "," + c.config.URLs[i]
			}
		}
	}
	conn, err := nats.Connect(urlsStr, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.conn = conn

	// Create components
	c.subscriber = NewSubscriber(c.logger.With("component", "subscriber"), c.config, conn)
	c.replyPublisher = NewReplyPublisher(c.logger.With("component", "reply-publisher"), c.config, conn)
	c.requester = NewRequester(c.logger.With("component", "requester"), c.config, conn)

	c.logger.Info("connected to NATS", "url", conn.ConnectedUrl())
	return nil
}

// Disconnect disconnects from NATS server
func (c *Client) Disconnect() {
	if c.conn != nil {
		c.logger.Info("disconnecting from NATS")
		c.conn.Close()
		c.conn = nil
	}
}

// GetSubscriber returns the subscriber component
func (c *Client) GetSubscriber() *Subscriber {
	return c.subscriber
}

// GetReplyPublisher returns the reply publisher component
func (c *Client) GetReplyPublisher() *ReplyPublisher {
	return c.replyPublisher
}

// GetRequester returns the requester component
func (c *Client) GetRequester() *Requester {
	return c.requester
}

// IsConnected returns true if connected to NATS
func (c *Client) IsConnected() bool {
	return c.conn != nil && c.conn.IsConnected()
}

// NewSubscriber creates a new NATS subscriber
func NewSubscriber(logger *slog.Logger, cfg *config.NATSConfig, conn *nats.Conn) *Subscriber {
	return &Subscriber{
		logger: logger,
		config: cfg,
		conn:   conn,
	}
}

// SubscribeSubjectsWithQueue subscribes to all configured NATS→TCP subjects with queue group.
func (s *Subscriber) SubscribeSubjectsWithQueue(subjects []string, queueGroup string, onMessage func(msg *nats.Msg)) error {
	s.onMessage = onMessage
	if len(subjects) == 0 {
		return fmt.Errorf("no NATS->TCP subjects configured")
	}

	for _, subject := range subjects {
		s.logger.Info("subscribing to NATS->TCP subject with queue group",
			"subject", subject,
			"queue_group", queueGroup)

		sub, err := s.conn.QueueSubscribe(subject, queueGroup, s.handleMessage)
		if err != nil {
			s.unsubscribeAll()
			return fmt.Errorf("failed to queue subscribe to %s: %w", subject, err)
		}
		s.subs = append(s.subs, sub)
	}

	s.logger.Info("subscribed to NATS->TCP subjects with queue group",
		"subject_count", len(s.subs),
		"queue_group", queueGroup)
	return nil
}

// Unsubscribe unsubscribes from all NATS→TCP subjects.
func (s *Subscriber) Unsubscribe() error {
	return s.unsubscribeAll()
}

func (s *Subscriber) unsubscribeAll() error {
	var firstErr error
	for _, sub := range s.subs {
		if sub == nil {
			continue
		}
		if err := sub.Unsubscribe(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.subs = nil
	return firstErr
}

// handleMessage handles incoming NATS messages
func (s *Subscriber) handleMessage(msg *nats.Msg) {
	s.logger.Debug("received external request",
		"subject", msg.Subject,
		"reply", msg.Reply,
		"size", len(msg.Data))

	if s.onMessage != nil {
		s.onMessage(msg)
	}
}

// NewReplyPublisher creates a new NATS reply publisher
func NewReplyPublisher(logger *slog.Logger, cfg *config.NATSConfig, conn *nats.Conn) *ReplyPublisher {
	return &ReplyPublisher{
		logger: logger,
		config: cfg,
		conn:   conn,
	}
}

// PublishReply publishes a reply to the given subject (A: NATS responses)
func (rp *ReplyPublisher) PublishReply(replySubject string, data []byte) error {
	if replySubject == "" {
		return fmt.Errorf("reply subject is empty")
	}

	rp.logger.Debug("publishing reply",
		"subject", replySubject,
		"size", len(data))

	if err := rp.conn.Publish(replySubject, data); err != nil {
		return fmt.Errorf("failed to publish reply to %s: %w", replySubject, err)
	}

	return nil
}

// PublishError publishes an error response to the given subject
func (rp *ReplyPublisher) PublishError(replySubject string, errMsg string) error {
	errorData := []byte(fmt.Sprintf(`{"error":"%s"}`, errMsg))
	return rp.PublishReply(replySubject, errorData)
}

// PublishTimeout publishes a timeout response to the given subject
func (rp *ReplyPublisher) PublishTimeout(replySubject string) error {
	timeoutData := []byte(`{"error":"timeout"}`)
	return rp.PublishReply(replySubject, timeoutData)
}

// NewRequester creates a new NATS requester
func NewRequester(logger *slog.Logger, cfg *config.NATSConfig, conn *nats.Conn) *Requester {
	return &Requester{
		logger: logger,
		config: cfg,
		conn:   conn,
	}
}

// RequestWithTimeoutToSubject sends a request to a specific subject with custom timeout
func (r *Requester) RequestWithTimeoutToSubject(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	r.logger.Debug("sending request to subject",
		"subject", subject,
		"size", len(data),
		"timeout", timeout)

	msg, err := r.conn.Request(subject, data, timeout)
	if err != nil {
		r.logger.Error("request to subject failed",
			"subject", subject,
			"error", err)
		return nil, fmt.Errorf("request to subject %s failed: %w", subject, err)
	}

	r.logger.Debug("received response from subject",
		"subject", subject,
		"size", len(msg.Data))

	return msg.Data, nil
}
