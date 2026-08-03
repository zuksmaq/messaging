// Package integration holds no library code: it exists so the
// end-to-end proof of the outbox → Kafka → Runner → inbox pattern has
// somewhere to live that may depend on every module at once.
//
// The library modules stay decoupled — the outbox knows nothing of
// Kafka and the inbox knows nothing of either — which means no one of
// them can host a test that wires all four together without taking on
// dependencies it has no other use for. This module takes them
// instead, and ships nothing.
//
// The tests are behind the integration build tag and stand up real
// infrastructure; see e2e_test.go.
package integration
