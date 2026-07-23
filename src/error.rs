use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Compression error: {0}")]
    Compression(#[source] Box<Self>),

    #[error("Deletion error: {0}")]
    Deletion(#[source] Box<Self>),

    #[error("Archive error: {0}")]
    Archive(String),
}

impl From<AppError> for std::io::Error {
    fn from(err: AppError) -> Self {
        match err {
            AppError::Io(e) => e,
            _ => Self::other(err.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
