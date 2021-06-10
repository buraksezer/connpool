package connpool

import (
	"net"
	"testing"
)

func TestConn_Impl(t *testing.T) {
	var _ net.Conn = new(PoolConn)
}
