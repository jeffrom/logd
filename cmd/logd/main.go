package main

import "github.com/jeffrom/logd"

func main() {
	config := logd.DefaultConfig
	srv := logd.NewServer("127.0.0.1:1774", config)
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}
