#[derive(Debug, thiserror::Error)]
pub enum CedisError {
    #[error("ERR {0}")]
    Generic(String),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArgCount(String),

    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    #[error("ERR value is not a valid float")]
    NotFloat,

    #[error("ERR no such key")]
    NoSuchKey,

    #[error("ERR syntax error")]
    SyntaxError,

    #[error("ERR unknown command '{0}', with args beginning with: {1}")]
    UnknownCommand(String, String),

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("LOADING Redis is loading the dataset in memory")]
    Loading,

    #[error("ERR {0}")]
    Protocol(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl CedisError {
    pub fn to_resp_error(&self) -> String {
        match self {
            CedisError::WrongType => {
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
            }
            other => format!("{other}"),
        }
    }
}

pub type CedisResult<T> = Result<T, CedisError>;
