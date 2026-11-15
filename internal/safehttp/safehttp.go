// Package safehttp provides an HTTP client for fetching URLs that come
// from untrusted input, such as a PDS endpoint named in a DID document.
package safehttp

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

// ErrBlocked reports a connection to an address that is not publicly
// routable.
var ErrBlocked = errors.New("safehttp: address is not public")

// Public reports whether ip is a publicly routable unicast address.
func Public(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast())
}

// NewClient returns a client that refuses to connect to loopback, private
// and link-local addresses. The check runs on the address actually dialed,
// after DNS resolution, so a hostname cannot be used to reach them, and it
// applies to every redirect hop.
func NewClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil || !Public(ip) {
				return fmt.Errorf("%w: %s", ErrBlocked, host)
			}
			return nil
		},
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: timeout,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       60 * time.Second,
		},
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("safehttp: too many redirects")
			}
			return nil
		},
	}
}
