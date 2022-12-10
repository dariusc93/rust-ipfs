use std::ops::{Deref, DerefMut};

use futures::stream::BoxStream;
use libp2p::{PeerId, kad::Record};

pub struct ProviderStream(pub BoxStream<'static, PeerId>);

impl Deref for ProviderStream {
    type Target = BoxStream<'static, PeerId>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ProviderStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl core::fmt::Debug for ProviderStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ProviderStream")
    }
}

pub struct RecordStream(pub BoxStream<'static, Record>);

impl Deref for RecordStream {
    type Target = BoxStream<'static, Record>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RecordStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl core::fmt::Debug for RecordStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "RecordStream")
    }
}
