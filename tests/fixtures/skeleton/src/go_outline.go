package outline

import "time"

// Server is an HTTP server.
type Server struct {
	Host    string `json:"host"`
	Port    int
	Timeout time.Duration
}

// Handler is an interface for request handlers.
type Handler interface {
	Handle(path string) (string, error)
	Close() error
}

// NewServer creates a new Server with the given host and port.
func NewServer(host string, port int) *Server {
	return &Server{Host: host, Port: port}
}

// Start starts the server and returns any error.
func (s *Server) Start() error {
	return nil
}

// Stop stops the server.
func (s *Server) Stop() {}
