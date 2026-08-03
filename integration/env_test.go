//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/zuksmaq/messaging"
	inboxpg "github.com/zuksmaq/messaging/inbox/postgres"
	"github.com/zuksmaq/messaging/kafka"
	"github.com/zuksmaq/messaging/kafka/consumer"
	"github.com/zuksmaq/messaging/kafka/producer"
	"github.com/zuksmaq/messaging/outbox"
	outboxpg "github.com/zuksmaq/messaging/outbox/postgres"
)

// The images the suite stands up.
const (
	postgresImage = "postgres:16-alpine"
	brokerImage   = "confluentinc/confluent-local:7.6.1"
)

// Timings. The relay polls far faster than its one-second default so the
// tests do not spend their time waiting for it; the two windows bound how
// long an assertion waits for something to happen, and how long it waits
// to be satisfied nothing will.
const (
	pollInterval = 20 * time.Millisecond

	// arrivalWindow bounds waiting for something that should happen. It
	// is generous because a consumer joining a group waits out a
	// rebalance first.
	arrivalWindow = 60 * time.Second

	// silenceWindow bounds waiting for something that should not happen —
	// a message the group has already committed past. It has to outlast a
	// rebalance too, or an idle consumer would look like a committed one.
	silenceWindow = 20 * time.Second
)

// The tests' stand-in business tables, on both sides of the broker.
//
// receipts has no unique constraint on purpose: a business effect applied
// twice shows up as a second row rather than being caught by the
// database, so the inbox is the only thing standing between a duplicate
// delivery and a duplicate effect.
const (
	createOrdersSQL   = `CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY)`
	createReceiptsSQL = `CREATE TABLE IF NOT EXISTS receipts (order_id TEXT NOT NULL)`
	insertOrderSQL    = `INSERT INTO orders (id) VALUES ($1)`
	insertReceiptSQL  = `INSERT INTO receipts (order_id) VALUES ($1)`
)

// The infrastructure the whole suite shares. One Postgres and one broker
// are started for the package rather than per test: every test here needs
// both, and container startup would otherwise dominate the run.
var (
	db        *sql.DB
	bootstrap string
)

func TestMain(m *testing.M) { os.Exit(runSuite(m)) }

// runSuite starts the containers, prepares the schema and runs the tests.
// It exists so the containers can be torn down with defer, which
// os.Exit's presence in TestMain rules out there.
func runSuite(m *testing.M) int {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("messaging"),
		tcpostgres.WithUsername("messaging"),
		tcpostgres.WithPassword("messaging"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return fail("starting postgres container: %v", err)
	}
	defer terminate(pg)

	broker, err := tckafka.Run(ctx, brokerImage)
	if err != nil {
		return fail("starting kafka container: %v", err)
	}
	defer terminate(broker)

	seeds, err := broker.Brokers(ctx)
	if err != nil {
		return fail("resolving broker addresses: %v", err)
	}
	bootstrap = seeds[0]

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fail("resolving connection string: %v", err)
	}
	if db, err = sql.Open("pgx", dsn); err != nil {
		return fail("opening database: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, stmt := range []string{
		outboxpg.CreateTableSQL,
		inboxpg.CreateTableSQL,
		createOrdersSQL,
		createReceiptsSQL,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fail("preparing schema: %v", err)
		}
	}

	return m.Run()
}

func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	return 1
}

func terminate(c testcontainers.Container) {
	if err := c.Terminate(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "terminating container: %v\n", err)
	}
}

// reset empties every table, so tests sharing the one database do not see
// each other's rows.
func reset(t *testing.T) {
	t.Helper()

	stmt := fmt.Sprintf(`TRUNCATE %s, %s, orders, receipts`, outboxpg.Table, inboxpg.Table)
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("resetting tables: %v", err)
	}
}

// placeOrder is the business write this whole library exists to serve: an
// order row and the event announcing it, committed together on one
// transaction. Nothing publishes here — the relay picks the row up.
func placeOrder(t *testing.T, ob *outbox.Outbox, topic, orderID string, payload []byte) {
	t.Helper()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, insertOrderSQL, orderID); err != nil {
		t.Fatalf("inserting order: %v", err)
	}
	if err := ob.Enqueue(ctx, tx, topic, []byte(orderID), payload, nil); err != nil {
		t.Fatalf("enqueueing: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

// newOutbox builds the producing half of the pattern against the Postgres
// dialect; the consuming half is in e2e_test.go with the handler that
// uses it.
func newOutbox(t *testing.T) *outbox.Outbox {
	t.Helper()

	ob, err := outbox.New(outboxpg.Dialect{})
	if err != nil {
		t.Fatalf("building outbox: %v", err)
	}
	return ob
}

// startRelay runs a relay publishing through p until the returned stop
// function is called. stop is idempotent and registered as a cleanup, so
// a failed assertion still ends the loop before the test's producer
// closes under it.
func startRelay(t *testing.T, p messaging.Producer[[]byte, []byte]) func() {
	t.Helper()

	r, err := outbox.NewRelay(outbox.RelayConfig{
		DB:           db,
		Dialect:      outboxpg.Dialect{},
		Producer:     p,
		PollInterval: pollInterval,
	})
	if err != nil {
		t.Fatalf("NewRelay = %v", err)
	}
	return runInBackground(t, "relay", r.Run)
}

// startRunner drives handler over c's messages until the returned stop
// function is called, with the default Halt policy: nothing in these
// tests is meant to be poison, so a message the handler rejects should
// stop the runner and be visible as one.
func startRunner(t *testing.T, c messaging.Consumer[[]byte, []byte], handler consumer.Handler[[]byte, []byte]) func() {
	t.Helper()

	return runInBackground(t, "runner", newRunner(t, c, handler).Run)
}

func newRunner(t *testing.T, c messaging.Consumer[[]byte, []byte], handler consumer.Handler[[]byte, []byte]) *consumer.Runner[[]byte, []byte] {
	t.Helper()

	r, err := consumer.NewRunner(c, handler, consumer.RunnerConfig{})
	if err != nil {
		t.Fatalf("NewRunner = %v", err)
	}
	return r
}

// runInBackground starts run and returns an idempotent stop function that
// cancels it and waits for it to return, asserting it returns nil.
func runInBackground(t *testing.T, what string, run func(context.Context) error) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("%s Run = %v, want nil after cancellation", what, err)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("%s Run did not return within 30s of cancellation", what)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// newProducer builds a real Kafka producer typed in bytes, which is what
// the relay publishes: the outbox stages serialized bytes and knows
// nothing of wire formats.
func newProducer(t *testing.T) *producer.Producer[[]byte, []byte] {
	t.Helper()

	p, err := producer.New[[]byte, []byte](producer.Config{
		BootstrapServers: bootstrap,
		KeyFormat:        kafka.FormatBytes,
		ValueFormat:      kafka.FormatBytes,
	})
	if err != nil {
		t.Fatalf("building producer: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(context.Background()); err != nil {
			t.Logf("closing producer: %v", err)
		}
	})
	return p
}

// newConsumer subscribes a real consumer to topic as part of group. The
// returned close function is idempotent and also registered as a
// cleanup: an assertion that needs the group empty has to be able to
// close it early, and closing twice is not safe.
func newConsumer(t *testing.T, group, topic string) (*consumer.Consumer[[]byte, []byte], func()) {
	t.Helper()

	c, err := consumer.New[[]byte, []byte](consumer.Config{
		BootstrapServers: bootstrap,
		GroupID:          group,
		Topics:           []string{topic},
		KeyFormat:        kafka.FormatBytes,
		ValueFormat:      kafka.FormatBytes,
	})
	if err != nil {
		t.Fatalf("building consumer: %v", err)
	}

	var once sync.Once
	closeConsumer := func() {
		once.Do(func() {
			if err := c.Close(); err != nil {
				t.Logf("closing consumer: %v", err)
			}
		})
	}
	t.Cleanup(closeConsumer)
	return c, closeConsumer
}

// createTopic creates the single-partition topic a test publishes to.
func createTopic(t *testing.T, topic string) {
	t.Helper()

	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []ckafka.TopicSpecification{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		t.Fatalf("creating topic %q: %v", topic, err)
	}
	for _, r := range results {
		if r.Error.Code() != ckafka.ErrNoError && r.Error.Code() != ckafka.ErrTopicAlreadyExists {
			t.Fatalf("creating topic %q: %v", topic, r.Error)
		}
	}
}

// countRows returns how many rows where matches, which is one of the
// suite's fixed queries.
func countRows(t *testing.T, what, query string, args ...any) int {
	t.Helper()

	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", what, err)
	}
	return n
}

func outboxRows(t *testing.T) int {
	t.Helper()
	return countRows(t, "outbox rows", `SELECT count(*) FROM `+outboxpg.Table)
}

func inboxRows(t *testing.T) int {
	t.Helper()
	return countRows(t, "inbox rows", `SELECT count(*) FROM `+inboxpg.Table)
}

// receiptsFor returns how many times the handler's business effect landed
// for orderID. Anything but one, after a delivery, is the bug the inbox
// exists to prevent.
func receiptsFor(t *testing.T, orderID string) int {
	t.Helper()
	return countRows(t, "receipts", `SELECT count(*) FROM receipts WHERE order_id = $1`, orderID)
}

// waitFor polls cond until it holds, failing the test if it has not
// within arrivalWindow. The relay is a background loop, so tests assert
// on where it gets to rather than on individual polls.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(arrivalWindow)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// awaitDeliveries reads the next n deliveries the handler reports.
func awaitDeliveries(t *testing.T, deliveries <-chan delivery, n int) []delivery {
	t.Helper()

	got := make([]delivery, 0, n)
	timeout := time.After(arrivalWindow)
	for len(got) < n {
		select {
		case d := <-deliveries:
			got = append(got, d)
		case <-timeout:
			t.Fatalf("handler saw %d deliveries within %s, want %d", len(got), arrivalWindow, n)
		}
	}
	return got
}

// assertCommittedPast asserts group has nothing left to read on topic, by
// joining the group again and finding nothing: an offset the runner
// committed is one a restart does not re-deliver.
//
// Call it only once the group's other consumer has closed — otherwise the
// two share a rebalance and reading nothing proves nothing.
func assertCommittedPast(t *testing.T, group, topic string) {
	t.Helper()

	if msg, ok := reJoin(t, group, topic, silenceWindow); ok {
		t.Errorf("re-joining group %q read %s@%d, want nothing: the offset should be committed past it",
			group, msg.Topic, msg.Offset)
	}
}

// assertRedelivers is assertCommittedPast's opposite: an offset the
// runner never committed must still be there for the next consumer of the
// group.
func assertRedelivers(t *testing.T, group, topic, wantEventID string) {
	t.Helper()

	msg, ok := reJoin(t, group, topic, arrivalWindow)
	if !ok {
		t.Fatalf("re-joining group %q read nothing within %s, want event %s re-delivered",
			group, arrivalWindow, wantEventID)
	}
	if msg.EventID() != wantEventID {
		t.Errorf("re-delivered event id = %q, want %q", msg.EventID(), wantEventID)
	}
}

// reJoin joins group afresh and reports the first message it is given, if
// any arrives within window.
func reJoin(t *testing.T, group, topic string, window time.Duration) (messaging.ReceivedMessage[[]byte, []byte], bool) {
	t.Helper()

	c, closeConsumer := newConsumer(t, group, topic)
	defer closeConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	msg, err := c.Consume(ctx)
	switch {
	case err == nil:
		return msg, true
	case errors.Is(err, context.DeadlineExceeded):
		return msg, false
	default:
		t.Fatalf("consuming as group %q: %v", group, err)
		return msg, false
	}
}
