// Automatically generated rust module for 'bitswap_pb.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use std::borrow::Cow;
use quick_protobuf::{MessageInfo, MessageRead, MessageWrite, BytesReader, Writer, WriterBackend, Result};
use quick_protobuf::sizeofs::*;
use super::*;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Message<'a> {
    pub wantlist: Option<bitswap_pb::mod_Message::Wantlist<'a>>,
    pub blocks: Vec<Cow<'a, [u8]>>,
    pub payload: Vec<bitswap_pb::mod_Message::Block<'a>>,
    pub blockPresences: Vec<bitswap_pb::mod_Message::BlockPresence<'a>>,
    pub pendingBytes: i32,
}

impl<'a> MessageRead<'a> for Message<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.wantlist = Some(r.read_message::<bitswap_pb::mod_Message::Wantlist>(bytes)?),
                Ok(18) => msg.blocks.push(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(26) => msg.payload.push(r.read_message::<bitswap_pb::mod_Message::Block>(bytes)?),
                Ok(34) => msg.blockPresences.push(r.read_message::<bitswap_pb::mod_Message::BlockPresence>(bytes)?),
                Ok(40) => msg.pendingBytes = r.read_int32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Message<'a> {
    fn get_size(&self) -> usize {
        0
        + self.wantlist.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
        + self.blocks.iter().map(|s| 1 + sizeof_len((s).len())).sum::<usize>()
        + self.payload.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + self.blockPresences.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + if self.pendingBytes == 0i32 { 0 } else { 1 + sizeof_varint(*(&self.pendingBytes) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if let Some(ref s) = self.wantlist { w.write_with_tag(10, |w| w.write_message(s))?; }
        for s in &self.blocks { w.write_with_tag(18, |w| w.write_bytes(&**s))?; }
        for s in &self.payload { w.write_with_tag(26, |w| w.write_message(s))?; }
        for s in &self.blockPresences { w.write_with_tag(34, |w| w.write_message(s))?; }
        if self.pendingBytes != 0i32 { w.write_with_tag(40, |w| w.write_int32(*&self.pendingBytes))?; }
        Ok(())
    }
}

pub mod mod_Message {

use std::borrow::Cow;
use super::*;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Wantlist<'a> {
    pub entries: Vec<bitswap_pb::mod_Message::mod_Wantlist::Entry<'a>>,
    pub full: bool,
}

impl<'a> MessageRead<'a> for Wantlist<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.entries.push(r.read_message::<bitswap_pb::mod_Message::mod_Wantlist::Entry>(bytes)?),
                Ok(16) => msg.full = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Wantlist<'a> {
    fn get_size(&self) -> usize {
        0
        + self.entries.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + if self.full == false { 0 } else { 1 + sizeof_varint(*(&self.full) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.entries { w.write_with_tag(10, |w| w.write_message(s))?; }
        if self.full != false { w.write_with_tag(16, |w| w.write_bool(*&self.full))?; }
        Ok(())
    }
}

pub mod mod_Wantlist {

use std::borrow::Cow;
use super::*;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Entry<'a> {
    pub block: Cow<'a, [u8]>,
    pub priority: i32,
    pub cancel: bool,
    pub wantType: bitswap_pb::mod_Message::mod_Wantlist::WantType,
    pub sendDontHave: bool,
}

impl<'a> MessageRead<'a> for Entry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.block = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.priority = r.read_int32(bytes)?,
                Ok(24) => msg.cancel = r.read_bool(bytes)?,
                Ok(32) => msg.wantType = r.read_enum(bytes)?,
                Ok(40) => msg.sendDontHave = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Entry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.block == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.block).len()) }
        + if self.priority == 0i32 { 0 } else { 1 + sizeof_varint(*(&self.priority) as u64) }
        + if self.cancel == false { 0 } else { 1 + sizeof_varint(*(&self.cancel) as u64) }
        + if self.wantType == bitswap_pb::mod_Message::mod_Wantlist::WantType::Block { 0 } else { 1 + sizeof_varint(*(&self.wantType) as u64) }
        + if self.sendDontHave == false { 0 } else { 1 + sizeof_varint(*(&self.sendDontHave) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.block != Cow::Borrowed(b"") { w.write_with_tag(10, |w| w.write_bytes(&**&self.block))?; }
        if self.priority != 0i32 { w.write_with_tag(16, |w| w.write_int32(*&self.priority))?; }
        if self.cancel != false { w.write_with_tag(24, |w| w.write_bool(*&self.cancel))?; }
        if self.wantType != bitswap_pb::mod_Message::mod_Wantlist::WantType::Block { w.write_with_tag(32, |w| w.write_enum(*&self.wantType as i32))?; }
        if self.sendDontHave != false { w.write_with_tag(40, |w| w.write_bool(*&self.sendDontHave))?; }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum WantType {
    Block = 0,
    Have = 1,
}

impl Default for WantType {
    fn default() -> Self {
        WantType::Block
    }
}

impl From<i32> for WantType {
    fn from(i: i32) -> Self {
        match i {
            0 => WantType::Block,
            1 => WantType::Have,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for WantType {
    fn from(s: &'a str) -> Self {
        match s {
            "Block" => WantType::Block,
            "Have" => WantType::Have,
            _ => Self::default(),
        }
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Block<'a> {
    pub prefix: Cow<'a, [u8]>,
    pub data: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for Block<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.prefix = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.data = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Block<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.prefix == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.prefix).len()) }
        + if self.data == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.data).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.prefix != Cow::Borrowed(b"") { w.write_with_tag(10, |w| w.write_bytes(&**&self.prefix))?; }
        if self.data != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.data))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct BlockPresence<'a> {
    pub cid: Cow<'a, [u8]>,
    pub type_pb: bitswap_pb::mod_Message::BlockPresenceType,
}

impl<'a> MessageRead<'a> for BlockPresence<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.cid = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.type_pb = r.read_enum(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for BlockPresence<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.cid == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.cid).len()) }
        + if self.type_pb == bitswap_pb::mod_Message::BlockPresenceType::Have { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.cid != Cow::Borrowed(b"") { w.write_with_tag(10, |w| w.write_bytes(&**&self.cid))?; }
        if self.type_pb != bitswap_pb::mod_Message::BlockPresenceType::Have { w.write_with_tag(16, |w| w.write_enum(*&self.type_pb as i32))?; }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BlockPresenceType {
    Have = 0,
    DontHave = 1,
}

impl Default for BlockPresenceType {
    fn default() -> Self {
        BlockPresenceType::Have
    }
}

impl From<i32> for BlockPresenceType {
    fn from(i: i32) -> Self {
        match i {
            0 => BlockPresenceType::Have,
            1 => BlockPresenceType::DontHave,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for BlockPresenceType {
    fn from(s: &'a str) -> Self {
        match s {
            "Have" => BlockPresenceType::Have,
            "DontHave" => BlockPresenceType::DontHave,
            _ => Self::default(),
        }
    }
}

}

