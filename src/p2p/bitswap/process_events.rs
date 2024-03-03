use crate::repo::Repo;

use super::message::{BitswapRequest, BitswapResponse, RequestType};

pub async fn handle_inbound_request(
    repo: &Repo,
    request: &BitswapRequest,
) -> Option<BitswapResponse> {
    if request.cancel {
        return None;
    }

    match request.ty {
        RequestType::Have => {
            let have = repo.contains(&request.cid).await.unwrap_or_default();
            if have || request.send_dont_have {
                Some(BitswapResponse::Have(have))
            } else {
                None
            }
        }
        RequestType::Block => {
            let block = repo.get_block_now(&request.cid).await.unwrap_or_default();
            if let Some(data) = block.map(|b| b.data().to_vec()) {
                Some(BitswapResponse::Block(data))
            } else if request.send_dont_have {
                Some(BitswapResponse::Have(false))
            } else {
                None
            }
        }
    }
}
