package http

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"testing"

	"github.com/z5labs/sakuin"

	"github.com/gofiber/fiber/v2"
)

func newTestServer(s *sakuin.Service) *fiber.App {
	return NewServer(s, fiber.Config{
		DisableStartupMessage: true,
	})
}

func withObjectStore(objStore sakuin.ObjectStore) func(*sakuin.Config) {
	return func(cfg *sakuin.Config) { cfg.ObjectStore = objStore }
}

func withDocumentStore(docStore sakuin.DocumentStore) func(*sakuin.Config) {
	return func(cfg *sakuin.Config) { cfg.DocumentStore = docStore }
}

func startTestServer(t *testing.T, opts ...func(*sakuin.Config)) (string, error) {
	cfg := sakuin.Config{
		ObjectStore:   sakuin.NewInMemoryObjectStore(),
		DocumentStore: sakuin.NewInMemoryDocumentStore(),
		RandSrc:       rand.Reader,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	s := sakuin.New(cfg)
	app := NewServer(s, fiber.Config{
		DisableStartupMessage: true,
	})

	ls, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", err
	}

	go func() {
		app.Listener(ls)
	}()

	t.Cleanup(func() {
		app.Shutdown()
	})

	return ls.Addr().String(), nil
}

func readAll(rc io.ReadCloser) ([]byte, error) {
	defer rc.Close()

	return ioutil.ReadAll(rc)
}

func decodeJSON(t *testing.T, rc io.ReadCloser, v interface{}) bool {
	b, err := readAll(rc)
	if err != nil {
		t.Error(err)
		return false
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	err = dec.Decode(v)
	if err != nil {
		t.Error(err)
		return false
	}

	return true
}
