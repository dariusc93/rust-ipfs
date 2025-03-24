#[cfg(not(target_arch = "wasm32"))]
pub mod flatfs;
pub mod memory;

#[cfg(target_arch = "wasm32")]
pub mod idb;

pub mod either;
pub mod arc;