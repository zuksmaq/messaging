package messaging

import "testing"

type decodedValue struct {
	Field string
}

func TestReceivedMessage_Tombstone(t *testing.T) {
	t.Run("raw bytes value type", func(t *testing.T) {
		tombstone := ReceivedMessage[string, []byte]{RawValue: nil}
		if !tombstone.Tombstone() {
			t.Error("expected tombstone for nil RawValue")
		}

		notTombstone := ReceivedMessage[string, []byte]{RawValue: []byte("value")}
		if notTombstone.Tombstone() {
			t.Error("expected no tombstone for non-nil RawValue")
		}
	})

	t.Run("decoded struct value type", func(t *testing.T) {
		tombstone := ReceivedMessage[string, decodedValue]{RawValue: nil}
		if !tombstone.Tombstone() {
			t.Error("expected tombstone for nil RawValue even with zero-value decoded struct")
		}

		notTombstone := ReceivedMessage[string, decodedValue]{
			RawValue: []byte(`{"field":"x"}`),
			Message:  Message[string, decodedValue]{Value: decodedValue{Field: "x"}},
		}
		if notTombstone.Tombstone() {
			t.Error("expected no tombstone for non-nil RawValue")
		}
	})

	t.Run("any value type", func(t *testing.T) {
		tombstone := ReceivedMessage[string, any]{RawValue: nil}
		if !tombstone.Tombstone() {
			t.Error("expected tombstone for nil RawValue")
		}

		notTombstone := ReceivedMessage[string, any]{RawValue: []byte("value")}
		if notTombstone.Tombstone() {
			t.Error("expected no tombstone for non-nil RawValue")
		}
	})
}
