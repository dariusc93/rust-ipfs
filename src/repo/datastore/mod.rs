#[cfg(not(target_arch = "wasm32"))]
pub mod flatfs;
pub mod memory;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "redb_data_store")]
pub mod redb;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "sled_data_store")]
pub mod sled;

#[cfg(target_arch = "wasm32")]
pub mod idb;
