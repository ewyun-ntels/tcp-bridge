package worker

const (
	inboundStatusSuccess = "success"
	inboundStatusError   = "error"
	inboundStatusTimeout = "timeout"
	inboundStatusDropped = "dropped"
)

const (
	inboundReasonOK                     = "ok"
	inboundReasonQueueFull              = "queue_full"
	inboundReasonUnknownMessageType     = "unknown_message_type"
	inboundReasonNATSRequestTimeout     = "nats_request_timeout"
	inboundReasonNATSRequestFailed      = "nats_request_failed"
	inboundReasonTCPResponseWriteFailed = "tcp_response_write_failed"
	inboundReasonInflightExpired        = "inflight_expired"
)
