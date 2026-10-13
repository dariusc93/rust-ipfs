use idb::Transaction;

pub(super) async fn finish_transaction(
    transaction: Transaction,
) -> Result<(), Box<dyn std::error::Error>> {
    if transaction.await?.is_committed() {
        Ok(())
    } else {
        Err(std::io::Error::other("IndexedDB transaction was aborted").into())
    }
}
