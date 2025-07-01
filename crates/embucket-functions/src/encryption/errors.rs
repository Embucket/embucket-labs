use datafusion::error::DataFusionError;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[error_stack_trace::debug]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid argument types for function '{function_name}': {types}"))]
    InvalidArgumentTypes {
        function_name: String,
        types: String,
    },

    #[snafu(display("All arguments must have the same length"))]
    ArrayLengthMismatch,

    #[snafu(display("Malformed encryption method parameter: {method}"))]
    MalformedEncryptionMethod { method: String },

    #[snafu(display("Unsupported encryption algorithm: {algorithm}"))]
    UnsupportedEncryptionAlgorithm { algorithm: String },

    #[snafu(display("Unsupported encryption mode: {mode}"))]
    UnsupportedEncryptionMode { mode: String },

    #[snafu(display(
        "IV/Nonce of size {bits} bits needs to be of size of {expected_bits} bits for encryption mode {mode}"
    ))]
    InvalidIvSize {
        bits: usize,
        expected_bits: usize,
        mode: String,
    },

    #[snafu(display("Key size of {bits} bits not found for encryption algorithm {algorithm}"))]
    InvalidKeySize { bits: usize, algorithm: String },

    #[snafu(display("Invalid key length {length}. Supported lengths: 16, 24, 32 bytes"))]
    InvalidKeyLength { length: usize },

    #[snafu(display("Ciphertext too short to contain authentication tag"))]
    CiphertextTooShort,

    #[snafu(display("Failed to create cipher from key"))]
    CipherCreation,

    #[snafu(display("Encryption failed"))]
    EncryptionFailed,

    #[snafu(display("Decryption failed. Check encrypted data, key, AAD, or AEAD tag."))]
    DecryptionFailed,

    #[snafu(display("Decryption failed. Check encrypted data, key, AAD, or AEAD tag."))]
    NullIvForDecryption,

    #[snafu(display("DataFusion error: {message}"))]
    DataFusion { message: String },
}

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        Self::Execution(error.to_string())
    }
}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion {
            message: error.to_string(),
        }
    }
}
