package main

import (
	"fmt"

	"github.com/jeffrom/logd"
)

func main() {
	config := logd.DefaultConfig
	client, err := logd.DialConfig("127.0.0.1:1774", config)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(logd.NewCommand(logd.CmdPing))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", resp)
}
