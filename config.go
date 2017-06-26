package logd

// ServerConfig contains configuration variables
type ServerConfig struct {
	Verbose bool
	Logger  Logger
	// Server  Server
}

// NewServerConfig returns a new configuration object
func NewServerConfig() *ServerConfig {
	return &ServerConfig{}
}

// DefaultServerConfig is the default application config
var DefaultServerConfig *ServerConfig

func init() {
	DefaultServerConfig = NewServerConfig()
	DefaultServerConfig.Verbose = true

	logger := newMemLogger()
	logger.discard = true
	DefaultServerConfig.Logger = logger
}
