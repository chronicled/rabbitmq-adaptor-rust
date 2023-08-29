use std::collections::BTreeMap;

use lapin::{message::Delivery, protocol::basic::AMQPProperties, types::AMQPValue};
use tracing::{field, info_span, Span};

use crate::{
    tracing_propagation::correlate_trace_from_delivery, CAUSATION_ID_HEADER_KEY,
    CAUSATION_ID_TRACING_KEY, CORRELATION_ID_HEADER_KEY, CORRELATION_ID_TRACING_KEY,
};

pub(crate) fn record_chronicled_ids_in_span(headers: &BTreeMap<String, String>) {
    let span = Span::current();
    if let Some(correlation_id) = headers.get(CORRELATION_ID_HEADER_KEY) {
        span.record(CORRELATION_ID_TRACING_KEY, field::display(correlation_id));
    }
    if let Some(causation_id) = headers.get(CAUSATION_ID_HEADER_KEY) {
        span.record(CAUSATION_ID_TRACING_KEY, field::display(causation_id));
    }
}

fn key_from_properties(key: &str, properties: &AMQPProperties) -> Option<String> {
    properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get(key))
        .and_then(|value| {
            if let AMQPValue::LongString(value) = value {
                Some(value.to_string())
            } else {
                None
            }
        })
}

pub(crate) fn create_consumer_span_from_message(message: &Delivery) -> Span {
    let Delivery { properties, .. } = message;
    let correlation_id = key_from_properties(CORRELATION_ID_HEADER_KEY, properties);
    let causation_id = key_from_properties(CAUSATION_ID_HEADER_KEY, properties);
    let span = match (correlation_id, causation_id) {
        (Some(correlation_id), Some(causation_id)) => info_span!(
            "consumer_callback",
            "otel.kind" = "consumer",
            "messaging.system" = "rabbitmq",
            correlation_id = field::display(correlation_id),
            causation_id = field::display(causation_id)
        ),
        (Some(correlation_id), None) => info_span!(
            "consumer_callback",
            "otel.kind" = "consumer",
            "messaging.system" = "rabbitmq",
            correlation_id = field::display(correlation_id)
        ),
        (None, Some(causation_id)) => info_span!(
            "consumer_callback",
            "otel.kind" = "consumer",
            "messaging.system" = "rabbitmq",
            causation_id = field::display(causation_id)
        ),
        (None, None) => info_span!(
            "consumer_callback",
            "otel.kind" = "consumer",
            "messaging.system" = "rabbitmq"
        ),
    };
    correlate_trace_from_delivery(&span, message);
    span
}
