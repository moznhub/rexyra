// Package goredis drives Rexyra through go-redis (github.com/redis/go-redis/v9)
// the way an application would. See clients/README.md for how to run it.
package goredis

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Password is the password the tests start rexyra-server with.
const Password = "client-tests-password"

// server describes the server under test.
var server struct {
	// Addr is host:port of the server.
	Addr string
	// Password is the password to connect with ("" for none).
	Password string
	// External is true when the tests run against a server they did not
	// start (REXYRA_ENDPOINT), which is how they are checked against Redis.
	External bool
}

// TestMain starts one rexyra-server for the whole run, on a free port with an
// empty data folder, and stops it at the end.
//
// REXYRA_SERVER names the executable (default: target/release/rexyra-server.exe
// in the repository). REXYRA_ENDPOINT (host:port) and REXYRA_PASSWORD use a
// server that is already running instead; that server's databases are written
// to and database 5 is flushed.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if endpoint := os.Getenv("REXYRA_ENDPOINT"); endpoint != "" {
		server.Addr = endpoint
		server.Password = os.Getenv("REXYRA_PASSWORD")
		server.External = true
		return m.Run()
	}

	exe := os.Getenv("REXYRA_SERVER")
	if exe == "" {
		root, err := repositoryRoot()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		exe = filepath.Join(root, "target", "release", "rexyra-server.exe")
	}
	if _, err := os.Stat(exe); err != nil {
		fmt.Fprintf(os.Stderr, "rexyra-server not found at %s; build it with `cargo build --release -p rexyra-server` or set REXYRA_SERVER\n", exe)
		return 1
	}

	port, err := freePort()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	dataDir, err := os.MkdirTemp("", "rexyra-client-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dataDir)

	cmd := exec.Command(exe,
		"--port", strconv.Itoa(port), "--dir", dataDir, "--save", "",
		"--requirepass", Password)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "could not start rexyra-server: %v\n", err)
		return 1
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer func() {
		select {
		case <-exited:
		default:
			_ = cmd.Process.Kill()
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
			}
		}
	}()

	server.Addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	server.Password = Password
	if err := waitUntilListening(server.Addr, exited); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("repository root (Cargo.toml) not found")
		}
		dir = parent
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitUntilListening(addr string, exited <-chan error) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-exited:
			return fmt.Errorf("rexyra-server exited during start-up: %v", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("rexyra-server did not start listening within 30 seconds")
}

// skipOnRexyra skips a test that fails because of a known Rexyra difference.
// It still runs against an external server, so the test keeps being checked
// against Redis.
func skipOnRexyra(t *testing.T, reason string) {
	t.Helper()
	if !server.External {
		t.Skip("Rexyra: " + reason)
	}
}

// options returns the options every test connects with, as an application
// would: everything left at go-redis's defaults except the address, the
// password and the protocol (0 keeps the library default, RESP3).
func options(protocol int) *redis.Options {
	return &redis.Options{
		Addr:         server.Addr,
		Password:     server.Password,
		Protocol:     protocol,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// connect returns a client that is closed when the test ends, after checking
// that it can talk to the server.
func connect(t *testing.T, opt *redis.Options) *redis.Client {
	t.Helper()
	c := redis.NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	return c
}

// protocols lists the protocols the tests run with: the library default
// (RESP3 through HELLO 3) and RESP2.
var protocols = []struct {
	name     string
	protocol int
}{
	{"default", 0},
	{"resp2", 2},
}

// eachProtocol runs body once with each protocol, on a fresh client and with a
// fresh key prefix.
func eachProtocol(t *testing.T, body func(t *testing.T, c *redis.Client, p string)) {
	for _, pr := range protocols {
		t.Run(pr.name, func(t *testing.T) {
			defer guard(t)
			c := connect(t, options(pr.protocol))
			body(t, c, prefix())
		})
	}
}

// prefix returns a key prefix unique to one test.
func prefix() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "go:" + hex.EncodeToString(b) + ":"
}

func ctx() context.Context { return context.Background() }

// failure is what must panics with; guard turns it into a test failure.
type failure struct {
	where string
	err   error
}

// must returns v, or stops the test when err is not nil. It needs a deferred
// guard(t) in the test (eachProtocol adds one).
func must[T any](v T, err error) T {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		panic(failure{fmt.Sprintf("%s:%d", filepath.Base(file), line), err})
	}
	return v
}

// must2 is must for calls that return two values and an error.
func must2[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		panic(failure{fmt.Sprintf("%s:%d", filepath.Base(file), line), err})
	}
	return a, b
}

// guard reports a failed must as a test failure. Use it as `defer guard(t)`.
func guard(t *testing.T) {
	if r := recover(); r != nil {
		if f, ok := r.(failure); ok {
			t.Fatalf("%s: unexpected error: %v", f.where, f.err)
		}
		panic(r)
	}
}

func equal[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// recordingProxy forwards one TCP address to another and keeps every command
// the client sent, so a test can see what go-redis sends on connect.
type recordingProxy struct {
	Addr     string
	listener net.Listener
	mu       sync.Mutex
	commands [][]string
}

func newRecordingProxy(t *testing.T, target string) *recordingProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &recordingProxy{Addr: l.Addr().String(), listener: l}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			go func() {
				defer upstream.Close()
				defer client.Close()
				_, _ = io.Copy(client, upstream)
			}()
			go func() {
				defer upstream.Close()
				defer client.Close()
				p.record(io.TeeReader(client, upstream))
			}()
		}
	}()
	return p
}

// record parses RESP arrays of bulk strings (the only form clients send).
func (p *recordingProxy) record(r io.Reader) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			_, _ = io.Copy(io.Discard, br)
			return
		}
		if !strings.HasPrefix(line, "*") {
			continue
		}
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		args := make([]string, 0, n)
		for i := 0; i < n; i++ {
			header, err := br.ReadString('\n')
			if err != nil {
				return
			}
			size, _ := strconv.Atoi(strings.TrimSpace(header[1:]))
			buf := make([]byte, size+2)
			if _, err := io.ReadFull(br, buf); err != nil {
				return
			}
			args = append(args, string(buf[:size]))
		}
		p.mu.Lock()
		p.commands = append(p.commands, args)
		p.mu.Unlock()
	}
}

// Commands returns the commands recorded so far, with AUTH/HELLO passwords hidden.
func (p *recordingProxy) Commands() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.commands))
	for _, c := range p.commands {
		masked := append([]string(nil), c...)
		for i := range masked {
			if masked[i] == server.Password && server.Password != "" {
				masked[i] = "<password>"
			}
		}
		out = append(out, strings.Join(masked, " "))
	}
	return out
}
