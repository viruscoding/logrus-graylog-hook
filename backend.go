package graylog

type Backend interface {
	// SendMessage write a message to the backend
	SendMessage(message *GELFMessage) error
	// ReadMessage read a message from the backend
	ReadMessage() (*GELFMessage, error)
}
