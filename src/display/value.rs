use std::fmt::Display;

use lapin::types::AMQPValue;

pub struct DisplayValue<'a>(pub &'a AMQPValue);

impl<'a> Display for DisplayValue<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.0;
        match value {
            AMQPValue::Boolean(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::ShortShortInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::ShortShortUInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::ShortInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::ShortUInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::LongInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::LongUInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::LongLongInt(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::Float(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::Double(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::DecimalValue(val) => f.write_fmt(format_args!("{:?}", val)),
            AMQPValue::ShortString(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::LongString(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::FieldArray(val) => f.write_fmt(format_args!("{:?}", val)),
            AMQPValue::Timestamp(val) => f.write_fmt(format_args!("{}", val)),
            AMQPValue::FieldTable(val) => f.write_fmt(format_args!("{:?}", val)),
            AMQPValue::ByteArray(val) => f.write_fmt(format_args!("{:?}", val)),
            AMQPValue::Void => f.write_str("Void"),
        }
    }
}
