use super::properties::DisplayProperties;
use lapin::message::Delivery;
use std::{fmt::Display, str::from_utf8};

pub struct DisplayDeliveryOption<'a>(pub Option<DisplayDelivery<'a>>);
pub struct DisplayDelivery<'a>(pub &'a Delivery);
impl<'a> Display for DisplayDelivery<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Exchange: {}, Routing Key: {}, Data: {}, Redelivered: {}, Properties: [{}]",
            self.0.exchange,
            self.0.routing_key,
            DisplayData(&self.0.data),
            self.0.redelivered,
            DisplayProperties(&self.0.properties)
        ))
    }
}

impl<'a> From<&'a Option<Delivery>> for DisplayDeliveryOption<'a> {
    fn from(value: &'a Option<Delivery>) -> Self {
        DisplayDeliveryOption(value.as_ref().map(DisplayDelivery))
    }
}

impl<'a> Display for DisplayDeliveryOption<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(delivery) = self.0.as_ref() {
            f.write_fmt(format_args!("{delivery}"))
        } else {
            f.write_str("None")
        }
    }
}

pub struct DisplayData<'a>(pub &'a Vec<u8>);

impl<'a> Display for DisplayData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match from_utf8(self.0) {
            Ok(utf8_data) => f.write_str(utf8_data),
            Err(_) => {
                let hex_encoded = hex::encode(self.0);
                f.write_str("0x").and_then(|_| f.write_str(&hex_encoded))
            }
        }
    }
}

#[cfg(test)]
mod test_display_delivery {
    use lapin::types::ShortString;

    use super::*;

    #[test]
    fn test_parseable_utf8() {
        let test_delivery = Delivery {
            delivery_tag: 0,
            exchange: ShortString::from("foo"),
            routing_key: ShortString::from("bar"),
            redelivered: false,
            properties: Default::default(),
            data: "foobar".as_bytes().to_vec(),
        };

        assert_eq!(
            format!(
                "{}",
                DisplayDeliveryOption(Some(DisplayDelivery(&test_delivery)))
            ),
            "Exchange: foo, Routing Key: bar, Data: foobar, Redelivered: false, Properties: []"
        )
    }

    #[test]
    fn test_unparseable_utf8() {
        let test_delivery = Delivery {
            delivery_tag: 0,
            exchange: ShortString::from("foo"),
            routing_key: ShortString::from("bar"),
            redelivered: false,
            properties: Default::default(),
            data: vec![0xed, 0xa0, 0x80],
        };

        assert_eq!(
            format!(
                "{}",
                DisplayDeliveryOption(Some(DisplayDelivery(&test_delivery)))
            ),
            "Exchange: foo, Routing Key: bar, Data: 0xeda080, Redelivered: false, Properties: []"
        )
    }

    #[test]
    fn test_no_message() {
        assert_eq!(format!("{}", DisplayDeliveryOption(None)), "None");
    }
}
