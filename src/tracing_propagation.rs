use lapin::{
    message::Delivery,
    types::{AMQPValue, ShortString},
};
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::Injector;
use std::collections::BTreeMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) struct AmqpClientCarrier<'a> {
    properties: &'a mut BTreeMap<String, String>,
}

impl<'a> AmqpClientCarrier<'a> {
    pub(crate) fn new(properties: &'a mut BTreeMap<String, String>) -> Self {
        Self { properties }
    }
}

impl<'a> Injector for AmqpClientCarrier<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.properties.insert(key.to_string(), value);
    }
}

pub(crate) struct AmqpHeaderCarrier<'a> {
    headers: &'a BTreeMap<ShortString, AMQPValue>,
}

impl<'a> AmqpHeaderCarrier<'a> {
    pub(crate) fn new(headers: &'a BTreeMap<ShortString, AMQPValue>) -> Self {
        Self { headers }
    }
}

impl<'a> Extractor for AmqpHeaderCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|header_value| {
            if let AMQPValue::LongString(header_value) = header_value {
                Some(header_value.as_str())
            } else {
                None
            }
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|header| header.as_str()).collect()
    }
}

pub(crate) fn correlate_trace_from_delivery(span: &Span, delivery: &Delivery) {
    let headers = &delivery
        .properties
        .headers()
        .clone()
        .unwrap_or_default()
        .inner()
        .clone();
    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&AmqpHeaderCarrier::new(headers))
    });

    span.set_parent(parent_cx);
}
