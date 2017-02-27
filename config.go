package logd

// Config contains configuration variables
type Config struct {
	Verbose bool
	Logger  Logger
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}
