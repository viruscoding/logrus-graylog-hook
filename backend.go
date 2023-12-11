package graylog

type Backend interface {
	// SendMessage write a message to the backend
	SendMessage(message *GELFMessage) error

	// Close the backend
	Close() error

	// LaunchConsume start consuming messages from the backend
	LaunchConsume(func(message *GELFMessage) error) error
}
