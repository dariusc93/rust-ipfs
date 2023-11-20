use futures::stream::BoxStream;
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
    Cleanup {
        refs: BoxStream<'static, Cid>,
        response: Channel<Vec<Cid>>,
    },
    List {
        response: Channel<Vec<Cid>>,
    },
    Wipe {
        response: Channel<()>,
    },
}
