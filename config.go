package logd

// Config contains configuration variables
type Config struct {
	Verbose       bool
	Logger        Logger
	Hostport      string
	ServerTimeout uint
	ClientTimeout uint
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}

// DefaultConfig is the default application config
var DefaultConfig *Config

func init() {
	DefaultConfig = NewConfig()
	DefaultConfig.ServerTimeout = 500
	DefaultConfig.ClientTimeout = 500

	logger := newMemLogger()
	logger.discard = true
	DefaultConfig.Logger = logger
}
