package alerta

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"tcp-bridge/internal/config"
)

/*
{
  "resource":    "upmc-1",           // 필수 - 모니터링 대상  , key
  "event":       "PGConnectionState", // 필수 - 이벤트 이름   , key

  "environment": "Production",       // 기본값: ""  (ALLOWED_ENVIRONMENTS 설정에 따라 제한), key
  "severity":    "critical",         // 기본값: "normal"
  "status":      "open",             // 기본값: "open"  (보통 자동 결정, 직접 지정 드묾)

  "service":     ["PG-TEST"],        // 기본값: []   (문자열 배열)
  "group":       "Database",         // 기본값: "Misc"
  "value":       "CONNECTING",       // 기본값: null  (문자열)
  "text":        "peer=10.255.254.2", // 기본값: ""   (description)

  "correlate":   ["PGConnectionState", "PGStateChanged"], // 기본값: []  (연관 이벤트 목록)
  "tags":        ["pg", "upmc"],     // 기본값: []
  "attributes":  {"region": "kr"},   // 기본값: {}   (커스텀 키-값, . $ 불가)

  "origin":      "my-monitor/host1", // 기본값: "스크립트명/호스트명"
  "type":        "exceptionAlert",   // 기본값: "exceptionAlert"
  "createTime":  "2026-04-24T13:26:00.000Z", // 기본값: 현재시각(UTC)
  "timeout":     86400,              // 기본값: ALERT_TIMEOUT 설정값 (초, 정수)
  "rawData":     "...",              // 기본값: null  (원본 데이터 문자열)
  "customer":    "upmc-1:PG-TEST"    // 기본값: null  (단일 문자열)  , key
}
*/

const pgConnectionStateItem = "PGConnectionState"

// SendConnectionStateAlert sends a PGConnectionState alert for TCP connection state changes.
// READY → severity "normal" (errCode 비포함), 나머지 → severity "critical" (errCode 포함)
// 비동기(goroutine)로 전송하여 메인 흐름을 블로킹하지 않습니다.
func (c *Client) SendConnectionStateAlert(connID, connName, endpoint, state string, priority int) {
	if !c.cfg.Enabled {
		return
	}
	if state != config.ConnStateReady && state != config.ConnStateConnectFailed {
		return
	}

	c.mu.Lock()
	if c.lastAlertedState[connID] == state {
		c.mu.Unlock()
		return
	}
	c.lastAlertedState[connID] = state
	c.mu.Unlock()

	alertDef, ok := c.alertMap[pgConnectionStateItem]
	if !ok {
		return
	}

	severity := "critical"
	errCode := alertDef.ErrCode
	if state == config.ConnStateReady {
		severity = "normal"
		errCode = ""
	}

	displayName := alertConnectionName(connName, connID)
	attrs := c.pgConnectionStateAttributes(connID, displayName, endpoint, state, priority, errCode)

	c.sendAlert(
		alertDef.Item,
		displayName,
		c.sysID,
		alertDef.Item,
		severity,
		alertDef.Group,
		state,
		fmt.Sprintf("peer=%s state=%s connection=%s priority=%d", endpoint, state, displayName, priority),
		attrs,
	)
}

// SendShutdownAlert sends a PGConnectionState DISCONNECTED alert for graceful shutdown.
// 종료 직전 유실을 막기 위해 동기 전송합니다.
func (c *Client) SendShutdownAlert(ctx context.Context, connID, connName, endpoint string, priority int) error {
	if !c.cfg.Enabled {
		return nil
	}

	alertDef, ok := c.alertMap[pgConnectionStateItem]
	if !ok {
		return nil
	}

	displayName := alertConnectionName(connName, connID)
	attrs := c.pgConnectionStateAttributes(
		connID,
		displayName,
		endpoint,
		config.ConnStateDisconnected,
		priority,
		alertDef.ErrCode,
	)

	return c.sendAlertSync(
		ctx,
		alertDef.Item,
		displayName,
		c.sysID,
		alertDef.Item,
		"critical",
		alertDef.Group,
		config.ConnStateDisconnected,
		fmt.Sprintf("POD Down - connection=%s peer=%s priority=%d", displayName, endpoint, priority),
		attrs,
	)
}

func (c *Client) pgConnectionStateAttributes(connID, connName, endpoint, state string, priority int, errCode string) map[string]string {
	host, port, _ := net.SplitHostPort(endpoint)

	attrs := map[string]string{
		"sysId":    c.sysID,
		"connId":   connID,
		"connName": connName,
		"peerHost": host,
		"peerPort": port,
		"priority": strconv.Itoa(priority),
		"state":    state,
	}
	if errCode != "" {
		attrs["errCode"] = errCode
	}

	return attrs
}

func alertConnectionName(connName, connID string) string {
	if connName != "" {
		return connName
	}
	if connID != "" {
		return connID
	}
	return "unknown"
}
