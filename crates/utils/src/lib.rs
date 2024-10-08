use serde_json::de;
use serde_json::ser;
use slatedb::db::Db as SlateDb;
use slatedb::error::SlateDBError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SlateDB error: {0}")]
    DbError(SlateDBError),

    #[error("Serialize error: {0}")]
    SerializeError(serde_json::Error),

    #[error("Deserialize error: {0}")]
    DeserializeError(serde_json::Error),
}

type Result<T> = std::result::Result<T, Error>;

pub struct Db(SlateDb);

impl Db {
    pub fn new(db: SlateDb) -> Self {
        Self(db)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.0.delete(key.as_bytes()).await;
        Ok(())
    }

    pub async fn put<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let serialized = ser::to_vec(value).map_err(|e| Error::SerializeError(e))?;
        self.0.put(key.as_bytes(), serialized.as_ref()).await;
        Ok(())
    }

    pub async fn get<T: for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let value: Option<bytes::Bytes> = self
            .0
            .get(key.as_bytes())
            .await
            .map_err(|e| Error::DbError(e))?;
        value.map_or_else(
            || Ok(None),
            |bytes| de::from_slice(&bytes).map_err(|e| Error::DeserializeError(e)),
        )
    }

    pub async fn keys(&self, key: &str) -> Result<Vec<String>> {
        let keys: Option<Vec<String>> = self.get(key).await?;
        Ok(keys.unwrap_or_default())
    }

    pub async fn append(&self, key: &str, value: String) -> Result<()> {
        self.modify(key, |all_keys: &mut Vec<String>| {
            all_keys.push(value.clone());
        })
        .await?;
        Ok(())
    }

    pub async fn remove(&self, key: &str, value: &str) -> Result<()> {
        self.modify(key, |all_keys: &mut Vec<String>| {
            all_keys.retain(|key| *key != value);
        })
        .await?;
        Ok(())
    }

    // function that takes closure as argument
    // it reads value from the db, deserialize it and pass it to the closure
    // it then gets value from the clousre, serialize it and write it back to the db
    pub async fn modify<T>(&self, key: &str, f: impl Fn(&mut T)) -> Result<()>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Default,
    {
        let mut value: T = self.get(key).await?.unwrap_or_default();

        f(&mut value);

        self.put(key, &value).await?;

        Ok(())
    }
}

impl From<SlateDBError> for Error {
    fn from(e: SlateDBError) -> Self {
        Error::DbError(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerializeError(e)
    }
}

impl From<Error> for iceberg::Error {
    fn from(e: Error) -> Self {
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()).with_source(e)
    }
}
