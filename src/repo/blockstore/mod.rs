#[cfg(not(target_arch = "wasm32"))]
pub mod flatfs;
#[cfg(target_arch = "wasm32")]
pub mod idb;
pub mod memory;
