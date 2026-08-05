package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/zuksmaq/messaging"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Relay defaults.
const (
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
	defaultMaxAttempts  = 5
	defaultLeaseTimeout = 30 * time.Second
)

// RelayConfig configures the polling relay. DB, Dialect and Producer are
// mandatory; the rest have defaults.
type RelayConfig struct {
	// DB is the database holding the outbox table. The relay opens its
	// own transaction per batch, so this is a pool, not a caller's
	// transaction.
	DB *sql.DB

	// Dialect supplies the claim and delete SQL.
	Dialect Dialect

	// Producer publishes claimed rows. Rows are staged as bytes, so the
	// producer is typed in bytes.
	Producer messaging.Producer[[]byte, []byte]

	// BatchSize is the most rows one poll claims. Defaults to 100.
	BatchSize int

	// PollInterval is how long Run waits after a poll that did not fill
	// a batch. Defaults to one second.
	PollInterval time.Duration

	// MaxAttempts is how many publish failures a row tolerates before
	// it is quarantined: excluded from future claims so it stops
	// blocking rows staged after it. Defaults to 5.
	MaxAttempts int

	// LeaseTimeout bounds how long a claiming transaction may sit idle
	// before the database itself aborts it and releases the row locks
	// the claim took. This is what reclaims a batch a relay abandoned
	// mid-claim — killed outright, or its connection dropped invisibly
	// to a pooler — within a bounded, documented window, rather than
	// leaving the rows locked until something else notices the dead
	// connection. Defaults to 30 seconds.
	//
	// Only applied against a Dialect that supports it (Postgres, via
	// idle_in_transaction_session_timeout); a dialect without a
	// session-level timeout to set ignores it. See the Dialect
	// implementations' docs for what else, if anything, to configure on
	// the underlying connection for that database.
	//
	// The timeout applies for the whole claim-publish-delete
	// transaction, including time spent inside Producer.Produce, not
	// just idle-and-abandoned time: set it comfortably above the
	// producer's expected publish latency for a full batch, or a
	// slow-but-live relay gets its transaction aborted the same way a
	// dead one does.
	//
	// This is one half of the reclaim story; the other is bounding how
	// long the database takes to notice a claiming connection actually
	// died (as opposed to sitting idle) rather than waiting on it
	// indefinitely — configure TCP keepalive on the underlying driver's
	// connection for that (e.g. a custom net.Dialer set on pgx's
	// pgconn.Config.DialFunc, or go-mssqldb's "keepAlive" DSN parameter).
	LeaseTimeout time.Duration
}

// Validate reports whether the config is usable. NewRelay calls it and
// refuses to construct an invalid Relay.
func (c RelayConfig) Validate() error {
	if c.DB == nil {
		return fmt.Errorf("%w: a database is required", messaging.ErrInvalidConfig)
	}
	if c.Dialect == nil {
		return fmt.Errorf("%w: a dialect is required", messaging.ErrInvalidConfig)
	}
	if c.Producer == nil {
		return fmt.Errorf("%w: a producer is required", messaging.ErrInvalidConfig)
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("%w: batch size %d is negative", messaging.ErrInvalidConfig, c.BatchSize)
	}
	if c.PollInterval < 0 {
		return fmt.Errorf("%w: poll interval %s is negative", messaging.ErrInvalidConfig, c.PollInterval)
	}
	if c.MaxAttempts < 0 {
		return fmt.Errorf("%w: max attempts %d is negative", messaging.ErrInvalidConfig, c.MaxAttempts)
	}
	if c.LeaseTimeout < 0 {
		return fmt.Errorf("%w: lease timeout %s is negative", messaging.ErrInvalidConfig, c.LeaseTimeout)
	}
	return nil
}

// withDefaults returns a copy with zero-valued optional fields replaced
// by their defaults.
func (c RelayConfig) withDefaults() RelayConfig {
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.LeaseTimeout == 0 {
		c.LeaseTimeout = defaultLeaseTimeout
	}
	return c
}

// leaseDialect is implemented by a Dialect whose database supports
// bounding how long a claiming transaction may sit idle before the
// database aborts it itself, releasing the claim's row locks. It is
// optional: Relay checks for it with a type assertion rather than
// requiring every Dialect to implement it, so adding support for a new
// database's session timeout later is not a breaking change to Dialect.
type leaseDialect interface {
	// LeaseSQL returns the statement that sets the claiming
	// transaction's idle-abort timeout to d.
	LeaseSQL(d time.Duration) string
}

// Relay polls the outbox table and publishes staged rows at-least-once:
// it claims a batch under the dialect's row lock, publishes each row in
// id order, and deletes only those the producer confirmed
// messaging.Persisted. A row whose publish fails repeatedly is
// quarantined after MaxAttempts, so it stops blocking the rows staged
// after it.
//
// It is exposed as a blocking Run(ctx) error rather than a
// framework-managed service type; the caller starts it with
// go r.Run(ctx) or awaits it directly.
//
// Per-key publish ordering is guaranteed only when a single Relay
// instance runs against a given table. The dialect's claim SQL uses
// SKIP LOCKED / READPAST so a second instance claims rows the first
// has not locked rather than blocking on them: that lets two instances
// drain a table concurrently, but nothing stops one from claiming and
// publishing a same-key row the other left behind for a later batch.
// Running multiple instances against the same table is a deliberate
// throughput-for-ordering trade-off, not a bug.
//
// A claiming transaction's row locks are held by the database, not by
// the relay process, so a relay killed mid-batch (or whose connection
// drops invisibly to a pooler) leaves them locked until the database
// itself notices. RelayConfig.LeaseTimeout bounds that window on a
// Dialect that supports a session-level idle timeout (Postgres); see its
// doc comment for what to configure on the connection for a dialect that
// doesn't.
type Relay struct {
	cfg    RelayConfig
	logger *slog.Logger

	published   metric.Int64Counter
	failed      metric.Int64Counter
	quarantined metric.Int64Counter
}

// NewRelay builds a Relay from cfg. It returns an error wrapping
// messaging.ErrInvalidConfig if cfg is invalid.
func NewRelay(cfg RelayConfig, opts ...Option) (*Relay, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := resolveOptions(opts)
	r := &Relay{
		cfg:    cfg.withDefaults(),
		logger: o.Logger,
	}
	if err := r.initMetrics(o.Meter); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Relay) initMetrics(m metric.Meter) error {
	var err error
	if r.published, err = m.Int64Counter("messaging.outbox.published",
		metric.WithDescription("outbox rows published and deleted")); err != nil {
		return fmt.Errorf("creating published counter: %w", err)
	}
	if r.failed, err = m.Int64Counter("messaging.outbox.publish_failures",
		metric.WithDescription("outbox rows left staged because the publish was not confirmed")); err != nil {
		return fmt.Errorf("creating publish failure counter: %w", err)
	}
	if r.quarantined, err = m.Int64Counter("messaging.outbox.quarantined",
		metric.WithDescription("outbox rows quarantined after repeated publish failures")); err != nil {
		return fmt.Errorf("creating quarantined counter: %w", err)
	}
	return nil
}

// Run polls and publishes until ctx is cancelled, at which point it
// returns nil. It returns an error wrapping messaging.ErrInvalidConfig
// immediately if r was not built with NewRelay.
//
// Publish and database failures are logged, counted and retried on the
// next poll rather than ending the loop: the rows they concern stay
// staged, so a transient broker or database problem costs latency, not
// events.
func (r *Relay) Run(ctx context.Context) error {
	if err := r.cfg.Validate(); err != nil {
		return fmt.Errorf("relay was not constructed with NewRelay: %w", err)
	}

	timer := time.NewTimer(r.cfg.PollInterval)
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		published, err := r.publishBatch(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.logger.ErrorContext(ctx, "outbox poll failed", slog.String("error", err.Error()))
		case published == r.cfg.BatchSize:
			// A full batch means there is probably a backlog; drain it
			// without waiting out the poll interval.
			continue
		}

		timer.Reset(r.cfg.PollInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
	}
}

// publishBatch claims one batch and publishes it, returning how many
// rows were confirmed and deleted.
//
// The claim, the publishes, the deletes and any attempt/quarantine
// bookkeeping share one transaction: the row locks the dialect took are
// held until it commits, so a second relay never sees the rows this one
// is working on. Rolling back on the way out releases the locks and
// leaves unconfirmed rows staged.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.cfg.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning outbox transaction: %w", err)
	}
	// Rollback after a successful Commit is a no-op, so this only has an
	// effect on the paths that leave rows staged.
	defer func() { _ = tx.Rollback() }()

	if err := r.setLeaseTimeout(ctx, tx); err != nil {
		return 0, err
	}

	rows, err := r.claim(ctx, tx)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	published := 0
	progressed := false
	for _, row := range rows {
		publishErr := r.publish(ctx, tx, row)
		if publishErr == nil {
			published++
			progressed = true
			continue
		}

		quarantined, err := r.recordFailure(ctx, tx, row)
		if err != nil {
			return 0, err
		}
		progressed = true
		if quarantined {
			// A row that has exhausted its attempts no longer blocks
			// the rows staged after it: it is excluded from future
			// claims, so publishing past it here cannot reorder
			// anything it would still be claimed alongside.
			continue
		}
		// Stop at the first row that hasn't yet exhausted its
		// attempts: publishing past it would reorder events for its
		// key. It and everything after it stay staged for the next
		// poll.
		r.logger.WarnContext(ctx, "outbox row left staged",
			slog.Int64("id", row.ID),
			slog.String("topic", row.Topic),
			slog.Int("remaining_in_batch", len(rows)-published),
			slog.String("error", publishErr.Error()),
		)
		break
	}

	if !progressed {
		return 0, nil
	}
	if err := tx.Commit(); err != nil {
		// The rows were published but not deleted, so the next poll
		// republishes them: at-least-once, as documented.
		return 0, fmt.Errorf("committing outbox batch: %w", err)
	}
	if published > 0 {
		r.published.Add(ctx, int64(published))
	}
	return published, nil
}

// recordFailure counts and logs a publish failure against row, then
// either records another attempt or quarantines the row once it has
// exhausted MaxAttempts. Quarantining is never silent: it is logged and
// counted like every other poison outcome in this codebase.
func (r *Relay) recordFailure(ctx context.Context, tx *sql.Tx, row Row) (bool, error) {
	attrs := metric.WithAttributes(attribute.String("topic", row.Topic))
	r.failed.Add(ctx, 1, attrs)

	attempts := row.Attempts + 1
	if attempts < r.cfg.MaxAttempts {
		if _, err := tx.ExecContext(ctx, r.cfg.Dialect.IncrementAttemptsSQL(), row.ID); err != nil {
			return false, fmt.Errorf("recording outbox row %d failure: %w", row.ID, err)
		}
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, r.cfg.Dialect.QuarantineSQL(), row.ID); err != nil {
		return false, fmt.Errorf("quarantining outbox row %d: %w", row.ID, err)
	}
	r.quarantined.Add(ctx, 1, attrs)
	r.logger.WarnContext(ctx, "outbox row quarantined",
		slog.Int64("id", row.ID),
		slog.String("topic", row.Topic),
		slog.Int("attempts", attempts),
	)
	return true, nil
}

// setLeaseTimeout bounds how long tx may sit idle before the database
// aborts it, releasing the claim's row locks. It is a no-op unless the
// dialect supports a session-level timeout to set.
func (r *Relay) setLeaseTimeout(ctx context.Context, tx *sql.Tx) error {
	ld, ok := r.cfg.Dialect.(leaseDialect)
	if !ok {
		return nil
	}
	if _, err := tx.ExecContext(ctx, ld.LeaseSQL(r.cfg.LeaseTimeout)); err != nil {
		return fmt.Errorf("setting outbox claim lease timeout: %w", err)
	}
	return nil
}

// claim reads a locked batch in id order. The rows are fully read before
// returning, because tx cannot run the deletes while a result set on it
// is still open.
func (r *Relay) claim(ctx context.Context, tx *sql.Tx) ([]Row, error) {
	result, err := tx.QueryContext(ctx, r.cfg.Dialect.ClaimSQL(), r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("claiming outbox batch: %w", err)
	}
	defer func() { _ = result.Close() }()

	var rows []Row
	for result.Next() {
		var (
			row     Row
			headers []byte
		)
		if err := result.Scan(&row.ID, &row.Topic, &row.Key, &row.Value, &headers, &row.Attempts); err != nil {
			return nil, fmt.Errorf("scanning outbox row: %w", err)
		}
		if row.Headers, err = decodeHeaders(headers); err != nil {
			return nil, fmt.Errorf("outbox row %d: %w", row.ID, err)
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("reading outbox batch: %w", err)
	}
	return rows, nil
}

// publish sends one row and deletes it, but only once the producer
// reports messaging.Persisted. Anything less leaves the row for the next
// poll, so a possibly-lost message is retried rather than dropped.
func (r *Relay) publish(ctx context.Context, tx *sql.Tx, row Row) error {
	headers := make(map[string][]byte, len(row.Headers)+1)
	for k, v := range row.Headers {
		headers[k] = v
	}
	headers[messaging.EventIDHeader] = []byte(strconv.FormatInt(row.ID, 10))

	out, err := r.cfg.Producer.Produce(ctx, row.Topic, row.Key, row.Value, headers)
	if err != nil {
		return fmt.Errorf("publishing outbox row %d to %s: %w", row.ID, row.Topic, err)
	}
	if out.Status != messaging.Persisted {
		return fmt.Errorf("publishing outbox row %d to %s: delivery status %s, want %s",
			row.ID, row.Topic, out.Status, messaging.Persisted)
	}

	if _, err := tx.ExecContext(ctx, r.cfg.Dialect.DeleteSQL(), row.ID); err != nil {
		return fmt.Errorf("deleting published outbox row %d: %w", row.ID, err)
	}
	return nil
}
