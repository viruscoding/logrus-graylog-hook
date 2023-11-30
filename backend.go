package graylog

type Backend interface {
	// SendMessage write a message to the backend
	SendMessage(message *GELFMessage) error

	// Close the backend
	Close() error

	// LaunchConsumeSync start consuming messages from the backend
	LaunchConsumeSync(func(message *GELFMessage) error) error
}
