// Package kafka holds the settings and wire formats shared by the
// producer and consumer sub-packages. Construct a client from
// kafka/producer or kafka/consumer; this package on its own only
// describes how to reach a cluster and how to encode what travels over
// it.
package kafka

import (
	"fmt"

	"github.com/zuksmaq/messaging"
)

// SecurityProtocol selects the transport and authentication mechanism
// used to reach the brokers.
type SecurityProtocol string

const (
	// SecurityPlaintext is unauthenticated, unencrypted transport.
	SecurityPlaintext SecurityProtocol = "plaintext"
	// SecuritySSL is TLS transport without SASL.
	SecuritySSL SecurityProtocol = "ssl"
	// SecuritySASLPlaintext is SASL authentication over plaintext.
	SecuritySASLPlaintext SecurityProtocol = "sasl_plaintext"
	// SecuritySASLSSL is SASL authentication over TLS.
	SecuritySASLSSL SecurityProtocol = "sasl_ssl"
)

// Security holds the broker connection credentials. Mechanism,
// Username and Password are required for the SASL protocols and
// ignored otherwise.
type Security struct {
	Protocol  SecurityProtocol
	Mechanism string
	Username  string
	Password  string
	// CALocation is an optional path to a CA certificate bundle for
	// the TLS protocols; empty uses the system trust store.
	CALocation string
}

func (s Security) sasl() bool {
	return s.Protocol == SecuritySASLPlaintext || s.Protocol == SecuritySASLSSL
}

// Validate reports whether the security settings are self-consistent.
// An empty Protocol is valid: it selects SecurityPlaintext.
func (s Security) Validate() error {
	switch s.Protocol {
	case "", SecurityPlaintext, SecuritySSL, SecuritySASLPlaintext, SecuritySASLSSL:
	default:
		return fmt.Errorf("%w: unknown security protocol %q", messaging.ErrInvalidConfig, s.Protocol)
	}
	if s.sasl() {
		if s.Mechanism == "" {
			return fmt.Errorf("%w: security mechanism is required for protocol %q", messaging.ErrInvalidConfig, s.Protocol)
		}
		if s.Username == "" || s.Password == "" {
			return fmt.Errorf("%w: security username and password are required for protocol %q", messaging.ErrInvalidConfig, s.Protocol)
		}
	}
	return nil
}

// WithDefaults returns a copy with an empty Protocol replaced by
// SecurityPlaintext.
func (s Security) WithDefaults() Security {
	if s.Protocol == "" {
		s.Protocol = SecurityPlaintext
	}
	return s
}

// Settings returns the client settings these credentials imply, keyed by
// their librdkafka names. The producer and consumer packages merge them
// into the config map they hand to the client.
func (s Security) Settings() map[string]string {
	out := map[string]string{"security.protocol": string(s.Protocol)}
	if s.sasl() {
		out["sasl.mechanism"] = s.Mechanism
		out["sasl.username"] = s.Username
		out["sasl.password"] = s.Password
	}
	if s.CALocation != "" {
		out["ssl.ca.location"] = s.CALocation
	}
	return out
}
