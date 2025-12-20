package otelnats

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// startEmbeddedNATS starts an embedded NATS server for testing.
// The server is automatically shut down when the test completes.
func startEmbeddedNATS(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:           "127.0.0.1",
		Port:           -1, // Random available port
		NoLog:          true,
		NoSigs:         true,
		MaxControlLine: 4096,
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

// startEmbeddedNATSWithJetStream starts an embedded NATS server with JetStream enabled.
// The server is automatically shut down when the test completes.
func startEmbeddedNATSWithJetStream(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:           "127.0.0.1",
		Port:           -1, // Random available port
		NoLog:          true,
		NoSigs:         true,
		MaxControlLine: 4096,
		JetStream:      true,
		StoreDir:       t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

// connectToNATS creates a NATS connection to the given server.
// The connection is automatically closed when the test completes.
func connectToNATS(t *testing.T, ns *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)

	t.Cleanup(func() {
		nc.Close()
	})

	return nc
}

// requireMessage waits for a message on the subscription and returns it.
// Fails the test if no message is received within the timeout.
func requireMessage(t *testing.T, sub *nats.Subscription, timeout time.Duration) *nats.Msg {
	t.Helper()
	msg, err := sub.NextMsg(timeout)
	require.NoError(t, err, "expected message but none received within %v", timeout)
	return msg
}

// createJetStream creates a JetStream instance for the given connection.
func createJetStream(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// createTestStream creates a JetStream stream for testing with the given subject prefix.
// Returns the stream name for use with receiver options.
func createTestStream(t *testing.T, js jetstream.JetStream, prefix string) string {
	t.Helper()
	ctx := t.Context()
	streamName := "TEST_" + prefix
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{prefix + ".*"},
		Storage:  jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	return streamName
}
