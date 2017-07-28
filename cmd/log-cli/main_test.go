package main

import (
	"flag"
	"os"
	"testing"
)

var integrationTest *bool

func init() {
	integrationTest = flag.Bool("integrationTest", false, "run integration tests")
}

func getArgStart() int {
	for i, arg := range os.Args[1:] {
		if len(arg) == 2 && arg[0] == '-' && arg[1] == '-' {
			return i + 2
		}
	}
	return -1
}

// XXX this doesn't work with cli library api
func TestMain(t *testing.T) {
	if *integrationTest {
		runApp(os.Args)
		// args := os.Args[getArgStart():]
		// fmt.Printf("%+v (%d)\n", args, getArgStart())
		// cmd.RootCmd.SetArgs(args)
		// cmd.RootCmd.DebugFlags()
		// if err := cmd.RootCmd.Execute(); err != nil {
		// 	fmt.Println(err)
		// }
	}
}
