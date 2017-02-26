package logd

// Config contains configuration variables
type Config struct {
	Verbose bool
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{
		Verbose: true,
	}
}
