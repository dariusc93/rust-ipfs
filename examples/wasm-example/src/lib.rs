use libipld::ipld;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;
use rust_ipfs::{Multiaddr, Protocol};
use wasm_bindgen::prelude::*;
use web_sys::{Document, HtmlElement};

#[wasm_bindgen]
pub async fn run() -> Result<(), JsError> {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let body = Body::from_current_window()?;
    body.append_p("Ipfs block exchange test")?;

    let node_a = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr(Multiaddr::empty().with(Protocol::Memory(0)))
        .start()
        .await
        .unwrap();
    let node_b = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr(Multiaddr::empty().with(Protocol::Memory(0)))
        .start()
        .await
        .unwrap();

    let peer_id = node_a.keypair().public().to_peer_id();
    let peer_id_b = node_b.keypair().public().to_peer_id();

    body.append_p(&format!("Our Node (A): {peer_id}"))?;
    body.append_p(&format!("Our Node (B): {peer_id_b}"))?;

    let addrs = node_a.listening_addresses().await.unwrap();

    for addr in addrs {
        node_b.add_peer(peer_id, addr).await.unwrap();
    }

    node_b.connect(peer_id).await.unwrap();

    let block_a = ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = node_a.put_dag(block_a.clone()).await.unwrap();

    let block_b = node_b.get_dag(cid).await.unwrap();

    assert_eq!(block_b, block_a);

    body.append_p(&format!("Block from node A: {block_b:?}"))?;

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}

/// Borrowed from libp2p
struct Body {
    body: HtmlElement,
    document: Document,
}

impl Body {
    fn from_current_window() -> Result<Self, JsError> {
        // Use `web_sys`'s global `window` function to get a handle on the global
        // window object.
        let document = web_sys::window()
            .ok_or(js_error("no global `window` exists"))?
            .document()
            .ok_or(js_error("should have a document on window"))?;
        let body = document
            .body()
            .ok_or(js_error("document should have a body"))?;

        Ok(Self { body, document })
    }

    fn append_p(&self, msg: &str) -> Result<(), JsError> {
        let val = self
            .document
            .create_element("p")
            .map_err(|_| js_error("failed to create <p>"))?;
        val.set_text_content(Some(msg));
        self.body
            .append_child(&val)
            .map_err(|_| js_error("failed to append <p>"))?;

        Ok(())
    }
}

fn js_error(msg: &str) -> JsError {
    std::io::Error::new(std::io::ErrorKind::Other, msg).into()
}
