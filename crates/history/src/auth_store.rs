use crate::{SlateDBWorksheetsStore, Token, TokenId};
use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use std::str;

#[derive(Snafu, Debug)]
pub enum AuthStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding/updating token: {source}"))]
    TokenPut { source: embucket_utils::Error },

    #[snafu(display("Error getting token: {source}"))]
    TokenGet { source: embucket_utils::Error },

    #[snafu(display("Error deleting token: {source}"))]
    TokenDelete { source: embucket_utils::Error },

    #[snafu(display("Can't locate token by key: {message}"))]
    TokenNotFound { message: String },
}

pub type AuthStoreResult<T> = std::result::Result<T, AuthStoreError>;

#[async_trait]
pub trait AuthStore: std::fmt::Debug + Send + Sync {
    async fn put_token(&self, token: Token) -> AuthStoreResult<Token>;
    async fn get_token(&self, id: TokenId) -> AuthStoreResult<Token>;
    async fn delete_token(&self, id: TokenId) -> AuthStoreResult<()>;
}

#[async_trait]
impl AuthStore for SlateDBWorksheetsStore {
    async fn put_token(&self, token: Token) -> AuthStoreResult<Token> {
        self.db
            .put_iterable_entity(&token)
            .await
            .context(TokenPutSnafu)?;
        Ok(token)
    }

    async fn get_token(&self, id: TokenId) -> AuthStoreResult<Token> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Token::get_key(&id);
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        let res: Option<Token> = self.db.get(key_str).await.context(TokenGetSnafu)?;
        res.ok_or_else(|| AuthStoreError::TokenNotFound {
            message: key_str.to_string(),
        })
    }

    async fn delete_token(&self, id: TokenId) -> AuthStoreResult<()> {
        Ok(self
            .db
            .delete_key(Token::get_key(&id))
            .await
            .context(TokenDeleteSnafu)?)
    }
}
