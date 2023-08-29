use super::value::DisplayValue;
use lapin::BasicProperties;
use std::fmt::Display;

pub struct DisplayProperties<'a>(pub &'a BasicProperties);

impl<'a> Display for DisplayProperties<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut display_stanzas: Vec<String> = vec![];
        let properties = self.0.clone();
        if let Some(headers) = properties.headers() {
            let mut header_stanzas: Vec<String> = vec![];
            headers.into_iter().for_each(|(key, value)| {
                header_stanzas.push(format!("{}: {}", key, DisplayValue(value)));
            });
            display_stanzas.push(format!("Headers: [{}]", header_stanzas.join(", ")));
        }
        if let Some(message_id) = properties.message_id() {
            display_stanzas.push(format!("Message ID: {}", message_id));
        }
        if let Some(timestamp) = properties.timestamp() {
            display_stanzas.push(format!("Timestamp: {}", timestamp));
        }
        if let Some(priority) = properties.priority() {
            display_stanzas.push(format!("Priority: {}", priority));
        }
        if let Some(app_id) = properties.app_id() {
            display_stanzas.push(format!("App ID: {}", app_id));
        }
        if let Some(expiration) = properties.expiration() {
            display_stanzas.push(format!("Expiration: {}", expiration));
        }
        if let Some(correlation_id) = properties.correlation_id() {
            display_stanzas.push(format!("Correlation ID: {}", correlation_id));
        }
        if let Some(content_type) = properties.content_type() {
            display_stanzas.push(format!("Content Type: {}", content_type));
        }
        if let Some(content_encoding) = properties.content_encoding() {
            display_stanzas.push(format!("Content Encoding: {}", content_encoding));
        }
        if let Some(delivery_mode) = properties.delivery_mode() {
            display_stanzas.push(format!("Delivery Mode: {}", delivery_mode));
        }
        if let Some(kind) = properties.kind() {
            display_stanzas.push(format!("Kind: {}", kind));
        }
        if let Some(reply_to) = properties.reply_to() {
            display_stanzas.push(format!("Reply To: {}", reply_to));
        }
        if let Some(user_id) = properties.user_id() {
            display_stanzas.push(format!("User ID: {}", user_id));
        }
        f.write_str(&display_stanzas.join(", "))
    }
}

#[cfg(test)]
mod test_display_amqp_properties {
    use super::*;
    use lapin::types::AMQPValue;
    use maplit::btreemap;

    #[test]
    fn test_empty() {
        let test_properties = BasicProperties::default();
        let test_properties_display = DisplayProperties(&test_properties);
        assert_eq!(format!("{}", test_properties_display), "");
    }

    #[test]
    fn test_headers_single() {
        let test_properties =
            BasicProperties::default().with_headers(
                btreemap! {
                    "foo_key".to_string().into() => AMQPValue::ShortString("bar_value".to_string().into())
                }
                .into(),
            );
        assert_eq!(
            format!("{}", DisplayProperties(&test_properties)),
            "Headers: [foo_key: bar_value]"
        );
    }

    #[test]
    fn test_headers_multiple() {
        let test_properties =
            BasicProperties::default().with_headers(
                btreemap! {
                    "foo_key".to_string().into() => AMQPValue::ShortString("bar_value".to_string().into()),
                    "baz_key".to_string().into() => AMQPValue::ShortString("bing_value".to_string().into())
                }
                .into(),
            );
        assert_eq!(
            format!("{}", DisplayProperties(&test_properties)),
            "Headers: [baz_key: bing_value, foo_key: bar_value]" // BTreeMaps are sorted by key
        );
    }

    #[test]
    fn test_combo() {
        let test_properties = BasicProperties::default()
            .with_headers(
                btreemap! {
                    "foo_key".to_string().into() => AMQPValue::ShortString("bar_value".to_string().into())
                }
                .into(),
            )
            .with_app_id("test_app_id".into())
            .with_cluster_id("test_cluster_id".into())
            .with_content_encoding("test_content_encoding".into())
            .with_content_type("test_content_type".into())
            .with_correlation_id("test_correlation_id".into())
            .with_delivery_mode(2)
            .with_expiration("test_expiration".into())
            .with_kind("test_kind".into())
            .with_message_id("test_message_id".into())
            .with_priority(4)
            .with_reply_to("test_reply_to".into())
            .with_timestamp(100)
            .with_user_id("test_user_id".into()
        );
        assert_eq!(
            format!("{}", DisplayProperties(&test_properties)),
            "Headers: [foo_key: bar_value], Message ID: test_message_id, Timestamp: 100, Priority: 4, App ID: test_app_id, Expiration: test_expiration, Correlation ID: test_correlation_id, Content Type: test_content_type, Content Encoding: test_content_encoding, Delivery Mode: 2, Kind: test_kind, Reply To: test_reply_to, User ID: test_user_id"
        );
    }
}
