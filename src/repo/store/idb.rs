use idb::{ObjectStore, Transaction};

pub(super) struct WriteTransaction {
    transaction: Option<Transaction>,
}

impl WriteTransaction {
    pub(super) fn new(transaction: Transaction) -> Self {
        Self {
            transaction: Some(transaction),
        }
    }

    pub(super) fn object_store(&self, name: &str) -> Result<ObjectStore, idb::Error> {
        self.transaction
            .as_ref()
            .expect("transaction is open")
            .object_store(name)
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.abort();
        }
    }
}

pub(super) async fn finish_transaction(
    transaction: Transaction,
) -> Result<(), Box<dyn std::error::Error>> {
    if transaction.await?.is_committed() {
        Ok(())
    } else {
        Err(std::io::Error::other("IndexedDB transaction was aborted").into())
    }
}

pub(super) async fn finish_write_transaction<T>(
    mut transaction: WriteTransaction,
    result: Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let transaction = transaction.transaction.take().expect("transaction is open");
    match result {
        Ok(value) => {
            finish_transaction(transaction.commit()?).await?;
            Ok(value)
        }
        Err(error) => {
            if let Ok(abort) = transaction.abort() {
                let _ = abort.await;
            }
            Err(error)
        }
    }
}
