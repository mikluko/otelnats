package otelnats

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// resourceMetricsToProto converts SDK ResourceMetrics to OTLP MetricsData proto.
func resourceMetricsToProto(rm *metricdata.ResourceMetrics) *metricspb.MetricsData {
	if rm == nil {
		return &metricspb.MetricsData{}
	}

	protoRM := &metricspb.ResourceMetrics{
		Resource: metricResourceToProto(rm.Resource),
	}

	for _, sm := range rm.ScopeMetrics {
		protoSM := &metricspb.ScopeMetrics{
			Scope: &commonpb.InstrumentationScope{
				Name:    sm.Scope.Name,
				Version: sm.Scope.Version,
			},
		}

		for _, m := range sm.Metrics {
			protoSM.Metrics = append(protoSM.Metrics, metricToProto(m))
		}

		protoRM.ScopeMetrics = append(protoRM.ScopeMetrics, protoSM)
	}

	return &metricspb.MetricsData{
		ResourceMetrics: []*metricspb.ResourceMetrics{protoRM},
	}
}

func metricResourceToProto(res *resource.Resource) *resourcepb.Resource {
	r := &resourcepb.Resource{}
	if res == nil {
		return r
	}
	for _, kv := range res.Attributes() {
		r.Attributes = append(r.Attributes, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}
	return r
}

func metricToProto(m metricdata.Metrics) *metricspb.Metric {
	pm := &metricspb.Metric{
		Name:        m.Name,
		Description: m.Description,
		Unit:        m.Unit,
	}

	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		pm.Data = &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: int64DataPointsToProto(data.DataPoints),
			},
		}
	case metricdata.Gauge[float64]:
		pm.Data = &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: float64DataPointsToProto(data.DataPoints),
			},
		}
	case metricdata.Sum[int64]:
		pm.Data = &metricspb.Metric_Sum{
			Sum: &metricspb.Sum{
				DataPoints:             int64DataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
				IsMonotonic:            data.IsMonotonic,
			},
		}
	case metricdata.Sum[float64]:
		pm.Data = &metricspb.Metric_Sum{
			Sum: &metricspb.Sum{
				DataPoints:             float64DataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
				IsMonotonic:            data.IsMonotonic,
			},
		}
	case metricdata.Histogram[int64]:
		pm.Data = &metricspb.Metric_Histogram{
			Histogram: &metricspb.Histogram{
				DataPoints:             histogramDataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
			},
		}
	case metricdata.Histogram[float64]:
		pm.Data = &metricspb.Metric_Histogram{
			Histogram: &metricspb.Histogram{
				DataPoints:             histogramDataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
			},
		}
	case metricdata.ExponentialHistogram[int64]:
		pm.Data = &metricspb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints:             expHistogramDataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
			},
		}
	case metricdata.ExponentialHistogram[float64]:
		pm.Data = &metricspb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints:             expHistogramDataPointsToProto(data.DataPoints),
				AggregationTemporality: temporalityToProto(data.Temporality),
			},
		}
	case metricdata.Summary:
		pm.Data = &metricspb.Metric_Summary{
			Summary: &metricspb.Summary{
				DataPoints: summaryDataPointsToProto(data.DataPoints),
			},
		}
	}

	return pm
}

func temporalityToProto(t metricdata.Temporality) metricspb.AggregationTemporality {
	switch t {
	case metricdata.DeltaTemporality:
		return metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	case metricdata.CumulativeTemporality:
		return metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	default:
		return metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED
	}
}

func int64DataPointsToProto(dps []metricdata.DataPoint[int64]) []*metricspb.NumberDataPoint {
	result := make([]*metricspb.NumberDataPoint, len(dps))
	for i, dp := range dps {
		result[i] = &metricspb.NumberDataPoint{
			Attributes:        attributeSetToProto(dp.Attributes),
			StartTimeUnixNano: uint64(dp.StartTime.UnixNano()),
			TimeUnixNano:      uint64(dp.Time.UnixNano()),
			Value:             &metricspb.NumberDataPoint_AsInt{AsInt: dp.Value},
		}
	}
	return result
}

func float64DataPointsToProto(dps []metricdata.DataPoint[float64]) []*metricspb.NumberDataPoint {
	result := make([]*metricspb.NumberDataPoint, len(dps))
	for i, dp := range dps {
		result[i] = &metricspb.NumberDataPoint{
			Attributes:        attributeSetToProto(dp.Attributes),
			StartTimeUnixNano: uint64(dp.StartTime.UnixNano()),
			TimeUnixNano:      uint64(dp.Time.UnixNano()),
			Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: dp.Value},
		}
	}
	return result
}

func attributeSetToProto(attrs attribute.Set) []*commonpb.KeyValue {
	iter := attrs.Iter()
	result := make([]*commonpb.KeyValue, 0, attrs.Len())
	for iter.Next() {
		kv := iter.Attribute()
		result = append(result, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}
	return result
}

func histogramDataPointsToProto[N int64 | float64](dps []metricdata.HistogramDataPoint[N]) []*metricspb.HistogramDataPoint {
	result := make([]*metricspb.HistogramDataPoint, len(dps))
	for i, dp := range dps {
		hdp := &metricspb.HistogramDataPoint{
			Attributes:        attributeSetToProto(dp.Attributes),
			StartTimeUnixNano: uint64(dp.StartTime.UnixNano()),
			TimeUnixNano:      uint64(dp.Time.UnixNano()),
			Count:             dp.Count,
			Sum:               ptrFloat64(float64(dp.Sum)),
			BucketCounts:      dp.BucketCounts,
			ExplicitBounds:    dp.Bounds,
		}
		if minVal, ok := dp.Min.Value(); ok {
			hdp.Min = ptrFloat64(float64(minVal))
		}
		if maxVal, ok := dp.Max.Value(); ok {
			hdp.Max = ptrFloat64(float64(maxVal))
		}
		result[i] = hdp
	}
	return result
}

func expHistogramDataPointsToProto[N int64 | float64](dps []metricdata.ExponentialHistogramDataPoint[N]) []*metricspb.ExponentialHistogramDataPoint {
	result := make([]*metricspb.ExponentialHistogramDataPoint, len(dps))
	for i, dp := range dps {
		ehdp := &metricspb.ExponentialHistogramDataPoint{
			Attributes:        attributeSetToProto(dp.Attributes),
			StartTimeUnixNano: uint64(dp.StartTime.UnixNano()),
			TimeUnixNano:      uint64(dp.Time.UnixNano()),
			Count:             dp.Count,
			Sum:               ptrFloat64(float64(dp.Sum)),
			Scale:             dp.Scale,
			ZeroCount:         dp.ZeroCount,
			Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.PositiveBucket.Offset,
				BucketCounts: dp.PositiveBucket.Counts,
			},
			Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.NegativeBucket.Offset,
				BucketCounts: dp.NegativeBucket.Counts,
			},
		}
		if minVal, ok := dp.Min.Value(); ok {
			ehdp.Min = ptrFloat64(float64(minVal))
		}
		if maxVal, ok := dp.Max.Value(); ok {
			ehdp.Max = ptrFloat64(float64(maxVal))
		}
		result[i] = ehdp
	}
	return result
}

func summaryDataPointsToProto(dps []metricdata.SummaryDataPoint) []*metricspb.SummaryDataPoint {
	result := make([]*metricspb.SummaryDataPoint, len(dps))
	for i, dp := range dps {
		sdp := &metricspb.SummaryDataPoint{
			Attributes:        attributeSetToProto(dp.Attributes),
			StartTimeUnixNano: uint64(dp.StartTime.UnixNano()),
			TimeUnixNano:      uint64(dp.Time.UnixNano()),
			Count:             dp.Count,
			Sum:               dp.Sum,
		}
		for _, qv := range dp.QuantileValues {
			sdp.QuantileValues = append(sdp.QuantileValues, &metricspb.SummaryDataPoint_ValueAtQuantile{
				Quantile: qv.Quantile,
				Value:    qv.Value,
			})
		}
		result[i] = sdp
	}
	return result
}

func ptrFloat64(v float64) *float64 {
	return &v
}
