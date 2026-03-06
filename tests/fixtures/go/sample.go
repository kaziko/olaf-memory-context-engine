package sample

import (
	"fmt"
	"os"
)

const (
	MaxRetries, DefaultTimeout = 3, 30
)

const Version = "1.0.0"

type Config struct {
	Host string
	Port int
}

type Stringer interface {
	String() string
}

type StringAlias = string

func NewConfig(host string, port int) *Config {
	return &Config{Host: host, Port: port}
}

func (c *Config) Validate() error {
	if c.Port <= 0 {
		return fmt.Errorf("invalid port")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func Env(key string) string {
	return os.Getenv(key)
}
