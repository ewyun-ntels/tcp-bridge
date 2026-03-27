package alerta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"tcp-bridge/internal/config"
)

// 2026-03-27: Alerta 알람 클라이언트 추가
// - service: sysID에서 자동 생성
// - tags: event, group에서 자동 생성
// - severity: READY→normal, 나머지→critical

// alertPayload represents the JSON body sent to Alerta API
type alertPayload struct {
	Resource    string            `json:"resource"`
	Event       string            `json:"event"`
	Environment string            `json:"environment"`
	Severity    string            `json:"severity"`
	Service     []string          `json:"service"`
	Group       string            `json:"group"`
	Value       string            `json:"value"`
	Text        string            `json:"text"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
}

// Client sends alerts to the Alerta API
type Client struct {
	logger     *slog.Logger
	cfg        *config.AlertaConfig
	sysID      string
	httpClient *http.Client
	alertMap   map[string]config.AlertDef // event name → AlertDef
}

// NewClient creates a new Alerta client
func NewClient(logger *slog.Logger, cfg *config.AlertaConfig, sysID string) *Client {
	alertMap := make(map[string]config.AlertDef, len(cfg.Alerts))
	for _, a := range cfg.Alerts {
		alertMap[a.Event] = a
	}

	return &Client{
		logger: logger,
		cfg:    cfg,
		sysID:  sysID,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		alertMap: alertMap,
	}
}

// SendConnectionStateAlert sends an alert for TCP connection state changes.
// READY → severity "normal" (errCode 비포함), 나머지 → severity "critical" (errCode 포함)
// 비동기(goroutine)로 전송하여 메인 흐름을 블로킹하지 않습니다.
func (c *Client) SendConnectionStateAlert(connID, endpoint, state string, priority int) {
	if !c.cfg.Enabled {
		return
	}

	alertDef, ok := c.alertMap["PGConnectionState"]
	if !ok {
		return
	}

	severity := "critical"
	errCode := alertDef.ErrCode
	if state == config.ConnStateReady {
		severity = "normal"
		errCode = ""
	}

	host, port, _ := net.SplitHostPort(endpoint)

	attrs := map[string]string{
		"sysId":    c.sysID,
		"connId":   connID,
		"peerHost": host,
		"peerPort": port,
		"priority": strconv.Itoa(priority),
		"state":    state,
	}
	if errCode != "" {
		attrs["errCode"] = errCode
	}

	c.sendAlert(
		alertDef.Event,
		fmt.Sprintf("%s/%s", c.sysID, connID),
		severity,
		alertDef.Group,
		state,
		fmt.Sprintf("peer=%s state=%s connID=%s priority=%d", endpoint, state, connID, priority),
		attrs,
	)
}

// sendAlert posts an alert to the Alerta API asynchronously.
// service는 sysID, tags는 event+group에서 자동 생성
func (c *Client) sendAlert(event, resource, severity, group, value, text string, attrs map[string]string) {
	payload := alertPayload{
		Resource:    resource,
		Event:       event,
		Environment: c.cfg.Environment,
		Severity:    severity,
		Service:     []string{c.sysID},
		Group:       group,
		Value:       value,
		Text:        text,
		Tags:        []string{c.sysID, event, group},
		Attributes:  attrs,
	}

	go func() {
		body, err := json.Marshal(payload)
		if err != nil {
			c.logger.Error("failed to marshal alerta payload", "error", err)
			return
		}

		url := c.cfg.URL + "/alert"
		resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			c.logger.Warn("failed to send alerta alert",
				"error", err,
				"event", event,
				"resource", resource,
				"severity", severity)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.logger.Warn("alerta returned non-2xx status",
				"status", resp.StatusCode,
				"event", event,
				"resource", resource,
				"severity", severity)
			return
		}

		c.logger.Debug("alerta alert sent",
			"event", event,
			"resource", resource,
			"severity", severity,
			"value", value)
	}()
}

// SetSysID updates the sysID (called after SysID is generated)
func (c *Client) SetSysID(sysID string) {
	c.sysID = sysID
}

// FindAlert returns the AlertDef for a given event name
func (c *Client) FindAlert(event string) (config.AlertDef, bool) {
	def, ok := c.alertMap[event]
	return def, ok
}

// SendTestAlert sends a test alert to verify connectivity (동기 호출)
func (c *Client) SendTestAlert() error {
	if !c.cfg.Enabled {
		return nil
	}

	payload := alertPayload{
		Resource:    c.sysID + "/test",
		Event:       "TestAlert",
		Environment: c.cfg.Environment,
		Severity:    "informational",
		Service:     []string{c.sysID},
		Group:       "Test",
		Value:       "test",
		Text:        fmt.Sprintf("tcp-bridge alerta connectivity test at %s", time.Now().Format(time.RFC3339)),
		Tags:        []string{c.sysID, "TestAlert", "Test"},
		Attributes:  map[string]string{"sysId": c.sysID},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal test alert: %w", err)
	}

	url := c.cfg.URL + "/alert"
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to send test alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alerta returned status %d", resp.StatusCode)
	}

	return nil
}
