package test

import (
	"github.com/egorban/egtsServ/pkg/egtsserv"
	"testing"
)

func mockEgtsServer(t *testing.T, addr string) {
	egtsserv.Start(addr)
}
