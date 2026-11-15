package safehttp

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPublic(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700:4700::1111": true,
		"127.0.0.1": false, "::1": false, "10.1.2.3": false, "192.168.1.1": false,
		"172.16.0.1": false, "169.254.169.254": false, "0.0.0.0": false, "fe80::1": false, "fd00::1": false,
	} {
		if got := Public(net.ParseIP(addr)); got != want {
			t.Errorf("Public(%s) = %v, want %v", addr, got, want)
		}
	}
}

func TestClientRefusesLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	_, err := NewClient(5 * time.Second).Get(srv.URL)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("request to a loopback server: err = %v, want ErrBlocked", err)
	}
}
