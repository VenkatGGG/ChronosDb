package httpauth

import (
	"net"
	"net/http"
	"strings"
)

// Policy protects privileged HTTP surfaces with loopback-only access and/or a shared bearer token.
type Policy struct {
	BearerToken    string
	LoopbackOnly   bool
	Realm          string
	PublicPaths    []string
	PublicPrefixes []string
}

// Wrap applies the policy to the provided handler.
func (p Policy) Wrap(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if p.BearerToken != "" {
			if token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")); token == p.BearerToken {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+p.realm()+`"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if p.LoopbackOnly && !isLoopbackRemote(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WrapFunc applies the policy to one handler function.
func (p Policy) WrapFunc(next http.HandlerFunc) http.Handler {
	return p.Wrap(next)
}

func (p Policy) realm() string {
	if strings.TrimSpace(p.Realm) != "" {
		return p.Realm
	}
	return "chronosdb"
}

func (p Policy) isPublicPath(path string) bool {
	for _, candidate := range p.PublicPaths {
		if path == candidate {
			return true
		}
	}
	for _, prefix := range p.PublicPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func isLoopbackRemote(remoteAddr string) bool {
	host := remoteAddr
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
