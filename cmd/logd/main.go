package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"syscall"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/events"
	"github.com/jeffrom/logd/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ReleaseVersion = "none"
	ReleaseCommit  = "none"
	ReleaseDate    = "none"
)

var cfgFile string
var printVersion bool
var tmpConfig = config.New()
var traceFile = ""
var cpuProfile = ""

func init() {
	cobra.OnInitialize(initConfig)
	viper.SetEnvPrefix("logd")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	// NOTE this doesn't work unless you viper.Get flags
	viper.AutomaticEnv()

	pflags := RootCmd.PersistentFlags()
	pflags.StringVar(&cfgFile, "config", config.Default.File, "config `FILE`")

	pflags.BoolVar(&printVersion, "version", false, "print version")
	pflags.BoolVarP(&tmpConfig.Verbose, "verbose", "v", config.Default.Verbose, "print debug output")
	viper.BindPFlag("verbose", pflags.Lookup("verbose"))

	pflags.StringVar(&tmpConfig.Host, "host", config.Default.Host, "a `HOST:PORT` combination for the tcp server to listen on")
	viper.BindPFlag("host", pflags.Lookup("host"))

	pflags.StringVar(&tmpConfig.HttpHost, "http-host", config.Default.HttpHost, "a `HOST:PORT` combination for the http server to listen on")
	viper.BindPFlag("host", pflags.Lookup("host"))

	pflags.DurationVar(&tmpConfig.Timeout, "timeout", config.Default.Timeout, "duration to wait for requests to complete")
	viper.BindPFlag("timeout", pflags.Lookup("timeout"))

	pflags.DurationVar(&tmpConfig.IdleTimeout, "idle-timeout", config.Default.IdleTimeout, "duration to wait for idle connections to be closed")
	viper.BindPFlag("idle-timeout", pflags.Lookup("idle-timeout"))

	pflags.DurationVar(&tmpConfig.ShutdownTimeout, "shutdown-timeout", config.Default.ShutdownTimeout, "duration to wait for requests to complete while shutting down")
	viper.BindPFlag("shutdown-timeout", pflags.Lookup("shutdown-timeout"))

	pflags.StringVar(&tmpConfig.WorkDir, "workdir", config.Default.WorkDir, "working directory")
	viper.BindPFlag("workdir", pflags.Lookup("workdir"))

	pflags.IntVar(&tmpConfig.LogFileMode, "file-mode", config.Default.LogFileMode, "mode used for log files")
	viper.BindPFlag("file-mode", pflags.Lookup("file-mode"))

	pflags.IntVar(&tmpConfig.MaxBatchSize, "batch-size", config.Default.MaxBatchSize, "maximum size of batch in bytes")
	viper.BindPFlag("batch-size", pflags.Lookup("batch-size"))

	pflags.IntVar(&tmpConfig.PartitionSize, "partition-size", config.Default.PartitionSize, "maximum size of a partitions in bytes")
	viper.BindPFlag("partition-size", pflags.Lookup("partition-size"))

	pflags.IntVar(&tmpConfig.MaxPartitions, "partitions", config.Default.MaxPartitions, "maximum number of partitions per topic")
	viper.BindPFlag("partitions", pflags.Lookup("partitions"))

	pflags.IntVar(&tmpConfig.FlushBatches, "flush-batches", config.Default.FlushBatches, "number of batches to write before flushing")
	viper.BindPFlag("flush-batches", pflags.Lookup("flush-batches"))

	pflags.DurationVar(&tmpConfig.FlushInterval, "flush-interval", config.Default.FlushInterval, "amount of time to wait before flushing")
	viper.BindPFlag("flush-interval", pflags.Lookup("flush-interval"))

	pflags.IntVar(&tmpConfig.MaxTopics, "topics", config.Default.MaxTopics, "maximum number of allowed topics")
	viper.BindPFlag("topics", pflags.Lookup("max-topics"))

	pflags.StringArrayVar(&tmpConfig.TopicWhitelist, "allowed-topics", config.Default.TopicWhitelist, "allowed topics")
	viper.BindPFlag("allowed-topics", pflags.Lookup("allowed-topics"))

	pflags.StringVar(&traceFile, "trace", "", "save execution trace data")
	pflags.StringVar(&cpuProfile, "cpuprofile", "", "save cpu profiling data")
}

func initConfig() {
	if tmpConfig.File != "" {
		viper.SetConfigFile(tmpConfig.File)
	} else {
		viper.SetConfigName("logd")
		viper.AddConfigPath("/etc/logd")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Println("failed to read config:", err)
			os.Exit(1)
		}
	}
}

// RootCmd is the only entry point for the logd application
var RootCmd = &cobra.Command{
	Use:   "logd",
	Short: "logd - networked log transport",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if printVersion {
			fmt.Printf("version: %s, released: %s, commit: %s\n",
				ReleaseVersion, ReleaseDate, ReleaseCommit)
			return
		}
		if traceFile != "" {
			f, err := os.Create(traceFile)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			trace.Start(f)
			defer trace.Stop()
		}
		if cpuProfile != "" {
			f, err := os.Create(cpuProfile)
			if err != nil {
				panic(err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				panic(err)
			}
			defer pprof.StopCPUProfile()
		}

		conf := tmpConfig
		h := events.NewHandlers(conf)

		stopC := make(chan os.Signal, 1)
		signal.Notify(stopC, os.Interrupt, syscall.SIGTERM)
		go func() {
			for range stopC {
				log.Print("Caught signal. Exiting...")
				internal.LogError(h.Stop())
			}
		}()

		if err := h.Start(); err != nil {
			panic(err)
		}
	},
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
