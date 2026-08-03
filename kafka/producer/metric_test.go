package producer

import (
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// testMeter returns a Meter backed by a manual reader so tests can
// collect and assert on recorded instrument values.
func testMeter() (*sdkmetric.ManualReader, metric.Meter) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, provider.Meter("kafka_test")
}
