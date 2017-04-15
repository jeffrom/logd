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

var defaultConfig *Config

func init() {
	defaultConfig = NewConfig()
	defaultConfig.Verbose = true
}
