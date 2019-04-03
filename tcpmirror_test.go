package main

// 1) Att -> CS -> DB (auth)
// 2) Att <- CS (auth reply)
// 3) CS -> NDTP server (auth)

// 4) Att -> CS -> DB (nav)
// 5) Att <- CS (nav reply)

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	testInit()
	defer func() {
		if err := pool.Close(); err != nil {
			os.Exit(1)
		}
	}()
	return m.Run()
}
