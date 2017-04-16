package logd

// Config contains configuration variables
type Config struct {
	Verbose bool
	Logger  Logger
	// Server  Server
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}

// DefaultConfig is the default application config
var DefaultConfig *Config

func init() {
	DefaultConfig = NewConfig()
	DefaultConfig.Verbose = true
}
