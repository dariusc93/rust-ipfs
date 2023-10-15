use libipld::Cid;

use crate::{Block, Channel};

use super::{BlockPut, BlockRm, BlockRmError};

pub mod flatfs;
pub mod memory;

pub(crate) enum RepoBlockCommand {
    Contains {
        cid: Cid,
        response: Channel<bool>,
    },
    Get {
        cid: Cid,
        response: Channel<Option<Block>>,
    },
    PutBlock {
        block: Block,
        response: Channel<(Cid, BlockPut)>,
    },
    Remove {
        cid: Cid,
        response: Channel<Result<BlockRm, BlockRmError>>,
    },
    List {
        response: Channel<Vec<Cid>>,
    },
    Wipe {
        response: Channel<()>,
    },
}
