package logd

import "log"

func debugf(config *Config, msg string, args ...interface{}) {
	if config.Verbose {
		log.Printf(msg, args...)
	}
}
