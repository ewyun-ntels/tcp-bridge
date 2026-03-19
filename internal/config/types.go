package config

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Message types (4.1.1.d)
//
// 메시지 플로우:
//   - NATS → TCP 방향: 0x01, 0x03, 0x05, 0x09 (NATS에서 요청 수신 → TCP로 전송)
//   - TCP → NATS 방향: 0x07, 0x0b (TCP에서 요청 수신 → NATS로 전송)
const (
	// NATS → TCP 방향 (NATS에서 요청을 받아 TCP로 전송)
	MsgTypeHelloRequest         uint8 = 0x01 // NATS → TCP
	MsgTypeHelloResponse        uint8 = 0x02 // TCP → NATS (payload only)
	MsgTypePingRequest          uint8 = 0x03 // NATS → TCP
	MsgTypePingResponse         uint8 = 0x04 // TCP → NATS (payload only)
	MsgTypeSubsChangeRequest    uint8 = 0x05 // NATS → TCP
	MsgTypeSubsChangeResponse   uint8 = 0x06 // TCP → NATS (payload only)
	MsgTypeCellInfoNotiRequest  uint8 = 0x09 // NATS → TCP
	MsgTypeCellInfoNotiResponse uint8 = 0x0a // TCP → NATS (payload only)

	// TCP → NATS 방향 (TCP에서 요청을 받아 NATS로 전송)
	MsgTypeSubsInfoRequest  uint8 = 0x07 // TCP → NATS
	MsgTypeSubsInfoResponse uint8 = 0x08 // NATS → TCP (with header)
	MsgTypeSubsSyncRequest  uint8 = 0x0b // TCP → NATS
	MsgTypeSubsSyncResponse uint8 = 0x0c // NATS → TCP (with header)
)

// 하위 호환성을 위한 별칭
const (
	FrameTypeHello = MsgTypeHelloRequest
	FrameTypeAck   = MsgTypeHelloResponse
	FrameTypePing  = MsgTypePingRequest
	FrameTypePong  = MsgTypePingResponse
)

// Connection states
const (
	ConnStateDisconnected  = "DISCONNECTED"
	ConnStateConnecting    = "CONNECTING"
	ConnStateConnectFailed = "CONNECT_FAILED"
	ConnStateReady         = "READY"
)

// Frame represents a TCP message (4.1)
// Header: 8 bytes
//   - Byte 1: Extension Bit(1) + Protocol Version(2) + Reserved(5) = 0x00
//   - Byte 2: Message Type (1 byte)
//   - Byte 3-4: Body Length (2 bytes, Big Endian)
//   - Byte 5-8: Transaction Identifier (4 bytes, Big Endian)
//
// Body: JSON (variable length)
type Frame struct {
	Type    uint8  // Message Type (4.1.1.d)
	TID     uint32 // Transaction Identifier (4.1.1.f)
	Payload []byte // Body (JSON, 4.1.2)

	// Metadata (not serialized, for runtime use only)
	ConnectionID string // Which connection this frame came from (endpoint별 구분용)
	ReceivedAt   time.Time
	EnqueuedAt   time.Time
}

// SendJob represents a job in the send queue
type SendJob struct {
	TID        uint32 // Transaction ID
	Frame      *Frame // Frame to send
	TargetHint string // Target connection hint (primary/secondary)
}

// TCPReplyInfo contains information needed to reply via TCP
type TCPReplyInfo struct {
	ConnectionID string // Connection identifier
	FrameType    uint8  // Response frame type
}

// Serialize encodes a frame to bytes (4.1)
// Header (8 bytes):
//   - Byte 1: Extension Bit(1) + Protocol Version(2) + Reserved(5) = 0x00
//   - Byte 2: Message Type
//   - Byte 3-4: Body Length (Big Endian)
//   - Byte 5-8: Transaction Identifier (Big Endian)
//
// Body: variable length
func (f *Frame) Serialize() ([]byte, error) {
	headerSize := 8 // Header size (4.1.1)
	bodyLen := len(f.Payload)

	// Body Length는 2 bytes이므로 최대 65535
	if bodyLen > 0xFFFF {
		return nil, fmt.Errorf("body too large: %d bytes (max 65535)", bodyLen)
	}

	totalLen := headerSize + bodyLen
	buf := make([]byte, totalLen)

	// Byte 1: Extension Bit(0) + Protocol Version(0) + Reserved(0) = 0x00
	buf[0] = 0x00

	// Byte 2: Message Type (4.1.1.d)
	buf[1] = f.Type

	// Byte 3-4: Body Length (Big Endian, 4.1.1.e)
	binary.BigEndian.PutUint16(buf[2:4], uint16(bodyLen))

	// Byte 5-8: Transaction Identifier (Big Endian, 4.1.1.f)
	binary.BigEndian.PutUint32(buf[4:8], f.TID)

	// Body (4.1.2)
	copy(buf[8:], f.Payload)

	return buf, nil
}

// DeserializeFrame decodes bytes to a frame (4.1)
func DeserializeFrame(data []byte) (*Frame, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("frame too short: need at least 8 bytes, got %d", len(data))
	}

	frame := &Frame{}

	// Byte 1: Extension Bit + Protocol Version + Reserved (4.1.1.a,b,c)
	extVersion := (data[0] >> 7) & 0x01  // Extension Bit
	protocolVer := (data[0] >> 5) & 0x03 // Protocol Version (2 bits)

	// Validate protocol version (4.1.1.b.ii)
	if protocolVer != 0 {
		return nil, fmt.Errorf("unsupported protocol version: %d (expected 0)", protocolVer)
	}

	// Extension bit는 현재 버전에서 0이어야 함 (4.1.1.a.ii)
	if extVersion != 0 {
		return nil, fmt.Errorf("unsupported extension bit: %d (expected 0)", extVersion)
	}

	// Byte 2: Message Type (4.1.1.d)
	frame.Type = data[1]

	// Byte 3-4: Body Length (Big Endian, 4.1.1.e)
	bodyLen := binary.BigEndian.Uint16(data[2:4])

	// Byte 5-8: Transaction Identifier (Big Endian, 4.1.1.f)
	frame.TID = binary.BigEndian.Uint32(data[4:8])

	// Transaction Identifier는 0을 사용해서는 안됨 (4.1.1.f.iii)
	// 단, Ping/Hello는 예외로 허용할 수 있음

	// Validate body length
	if int(bodyLen) != len(data)-8 {
		return nil, fmt.Errorf("body length mismatch: expected %d, got %d", bodyLen, len(data)-8)
	}

	// Body (4.1.2)
	if bodyLen > 0 {
		frame.Payload = make([]byte, bodyLen)
		copy(frame.Payload, data[8:])
	}

	return frame, nil
}

// IsHandshake returns true if the frame is a handshake frame (Hello)
func (f *Frame) IsHandshake() bool {
	return f.Type == MsgTypeHelloRequest || f.Type == MsgTypeHelloResponse
}

// IsPing returns true if frame is a ping/pong frame
func (f *Frame) IsPing() bool {
	return f.Type == MsgTypePingRequest || f.Type == MsgTypePingResponse
}

// IsBusinessMessage returns true if the message is a business message (not Hello/Ping)
func (f *Frame) IsBusinessMessage() bool {
	return !f.IsHandshake() && !f.IsPing()
}

// HandshakeRequest represents the HELLO handshake request message
type HandshakeRequest struct {
	SysID      string `json:"sys-id"`      // 접속하는 Peer Name
	BranchName string `json:"branch-name"` // 국사명 (SS/DS/BR)
}

// KeyListEntry represents an encryption key entry
type KeyListEntry struct {
	Num    int    `json:"num"`     // key 번호
	KeyEnc string `json:"key_enc"` // 암호화된 key 값
	IVEnc  string `json:"iv_enc"`  // 암호화된 iv 값
	Salt   string `json:"salt"`    // salt 정보
}

// HandshakeResponse represents the ACK handshake response message
type HandshakeResponse struct {
	SysID        string         `json:"sys-id"`        // 응답하는 Peer Name
	Code         int            `json:"code"`          // 결과 코드 (200=성공)
	PingInterval int            `json:"ping-interval"` // Ping 메시지 전송 주기 (초, optional: 미존재 시 기본 5초)
	KeyList      []KeyListEntry `json:"keyList"`       // 암호화 key 목록 (optional)
	Cause        string         `json:"cause"`         // 실패 시 상세 사유 (optional)
}

// String returns a string representation of the frame
func (f *Frame) String() string {
	typeStr := "UNKNOWN"
	switch f.Type {
	case MsgTypeHelloRequest:
		typeStr = "HELLO-REQ"
	case MsgTypeHelloResponse:
		typeStr = "HELLO-RES"
	case MsgTypePingRequest:
		typeStr = "PING-REQ"
	case MsgTypePingResponse:
		typeStr = "PING-RES"
	case MsgTypeSubsChangeRequest:
		typeStr = "SUBS-CHANGE-REQ"
	case MsgTypeSubsChangeResponse:
		typeStr = "SUBS-CHANGE-RES"
	case MsgTypeSubsInfoRequest:
		typeStr = "SUBS-INFO-REQ"
	case MsgTypeSubsInfoResponse:
		typeStr = "SUBS-INFO-RES"
	case MsgTypeCellInfoNotiRequest:
		typeStr = "CELLINFO-NOTI-REQ"
	case MsgTypeCellInfoNotiResponse:
		typeStr = "CELLINFO-NOTI-RES"
	case MsgTypeSubsSyncRequest:
		typeStr = "SUBS-SYNC-REQ"
	case MsgTypeSubsSyncResponse:
		typeStr = "SUBS-SYNC-RES"
	default:
		typeStr = fmt.Sprintf("0x%02x", f.Type)
	}

	return fmt.Sprintf("Frame{Type:%s, TID:%d, PayloadLen:%d}", typeStr, f.TID, len(f.Payload))
}
