use datafusion::error::DataFusionError;
use snafu::Location;
use snafu::Snafu;
use std::array::TryFromSliceError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[error_stack_trace::debug]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid argument types for function '{function_name}': {types}"))]
    InvalidArgumentTypes {
        function_name: String,
        types: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("All arguments must have the same length"))]
    ArrayLengthMismatch {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Malformed encryption method parameter: {method}"))]
    MalformedEncryptionMethod {
        method: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported encryption algorithm: {algorithm}"))]
    UnsupportedEncryptionAlgorithm {
        algorithm: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported encryption mode: {mode}"))]
    UnsupportedEncryptionMode {
        mode: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "IV/Nonce of size {bits} bits needs to be of size of {expected_bits} bits for encryption mode {mode}"
    ))]
    InvalidIvSize {
        bits: usize,
        expected_bits: usize,
        mode: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Key size of {bits} bits not found for encryption algorithm {algorithm}"))]
    InvalidKeySize {
        bits: usize,
        algorithm: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid key length {length}. Supported lengths: 16, 24, 32 bytes"))]
    InvalidKeyLength {
        length: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Ciphertext too short to contain authentication tag"))]
    CiphertextTooShort {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create cipher from key"))]
    CipherCreation {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Encryption failed"))]
    EncryptionFailed {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Decryption failed. Check encrypted data, key, AAD, or AEAD tag."))]
    DecryptionFailed {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Decryption failed. Check encrypted data, key, AAD, or AEAD tag."))]
    NullIvForDecryption {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Slice creation falied: {error}"))]
    SliceCreationFailed {
        #[snafu(source)]
        error: TryFromSliceError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        Self::External(Box::new(error))
    }
}
