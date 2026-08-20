// Automatically generated rust module for 'ipns_pb.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use super::*;
use quick_protobuf::sizeofs::*;
use quick_protobuf::{BytesReader, MessageInfo, MessageRead, MessageWrite, Result, Writer, WriterBackend};
use std::borrow::Cow;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct IpnsEntry<'a> {
    pub value: Cow<'a, [u8]>,
    pub signatureV1: Cow<'a, [u8]>,
    pub validityType: Option<i32>,
    pub validity: Cow<'a, [u8]>,
    pub sequence: Option<u64>,
    pub ttl: Option<u64>,
    pub pubKey: Cow<'a, [u8]>,
    pub signatureV2: Cow<'a, [u8]>,
    pub data: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for IpnsEntry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.value = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.signatureV1 = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.validityType = Some(r.read_enum(bytes)?),
                Ok(34) => msg.validity = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(40) => msg.sequence = Some(r.read_uint64(bytes)?),
                Ok(48) => msg.ttl = Some(r.read_uint64(bytes)?),
                Ok(58) => msg.pubKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(66) => msg.signatureV2 = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(74) => msg.data = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for IpnsEntry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.value == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.value).len()) }
        + if self.signatureV1 == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signatureV1).len()) }
        + self.validityType.map_or(0, |value| 1 + sizeof_varint(value as u64))
        + if self.validity == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.validity).len()) }
        + self.sequence.map_or(0, |value| 1 + sizeof_varint(value))
        + self.ttl.map_or(0, |value| 1 + sizeof_varint(value))
        + if self.pubKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.pubKey).len()) }
        + if self.signatureV2 == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signatureV2).len()) }
        + if self.data == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.data).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.value != Cow::Borrowed(b"") { w.write_with_tag(10, |w| w.write_bytes(&**&self.value))?; }
        if self.signatureV1 != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.signatureV1))?; }
        if let Some(value) = self.validityType { w.write_with_tag(24, |w| w.write_enum(value))?; }
        if self.validity != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.validity))?; }
        if let Some(value) = self.sequence { w.write_with_tag(40, |w| w.write_uint64(value))?; }
        if let Some(value) = self.ttl { w.write_with_tag(48, |w| w.write_uint64(value))?; }
        if self.pubKey != Cow::Borrowed(b"") { w.write_with_tag(58, |w| w.write_bytes(&**&self.pubKey))?; }
        if self.signatureV2 != Cow::Borrowed(b"") { w.write_with_tag(66, |w| w.write_bytes(&**&self.signatureV2))?; }
        if self.data != Cow::Borrowed(b"") { w.write_with_tag(74, |w| w.write_bytes(&**&self.data))?; }
        Ok(())
    }
}

pub mod mod_IpnsEntry {


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(dead_code)]
pub enum ValidityType {
    EOL = 0,
}

impl Default for ValidityType {
    fn default() -> Self {
        ValidityType::EOL
    }
}

impl From<i32> for ValidityType {
    fn from(i: i32) -> Self {
        match i {
            0 => ValidityType::EOL,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for ValidityType {
    fn from(s: &'a str) -> Self {
        match s {
            "EOL" => ValidityType::EOL,
            _ => Self::default(),
        }
    }
}

}

#[allow(dead_code)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct IpnsSignatureV2Checker<'a> {
    pub pubKey: Cow<'a, [u8]>,
    pub signatureV2: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for IpnsSignatureV2Checker<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(58) => msg.pubKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(66) => msg.signatureV2 = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for IpnsSignatureV2Checker<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.pubKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.pubKey).len()) }
        + if self.signatureV2 == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signatureV2).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.pubKey != Cow::Borrowed(b"") { w.write_with_tag(58, |w| w.write_bytes(&**&self.pubKey))?; }
        if self.signatureV2 != Cow::Borrowed(b"") { w.write_with_tag(66, |w| w.write_bytes(&**&self.signatureV2))?; }
        Ok(())
    }
}
