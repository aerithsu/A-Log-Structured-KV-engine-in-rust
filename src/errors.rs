use log::error;
use std::result;
use thiserror::Error;

pub type Result<T> = result::Result<T, Errors>;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,
    #[error("failed to write to data file")]
    FailedToWriteToDataFile,
    #[error("failed to syn data file")]
    FailedSynDataFile,
    #[error("failed to open data file")]
    FailedToOpenDataFile,
    #[error("The key is empty")]
    KeyIsEmpty,
    #[error("Index update failed")]
    IndexUpdateFailed,
    #[error("key not found")]
    KeyNotFound,
    #[error("data file not found in the database")]
    DataFileNotFound,
    #[error("database dir path can't be empty")]
    DirPathIsEmpty,
    #[error("data file size too small")]
    DataFileSizeTooSmall,
    #[error("failed to create database directory")]
    FailedToCreateDatabaseDir,
    #[error("failed to read database dir")]
    FailedToReadDataBaseDir,
    #[error("the database directory maybe corrupted")]
    DataDirectoryCorrupted,
    #[error("read data file eof")]
    ReadDataFileEOF,
    #[error("invalid crc value,log record maybe corrupted")]
    InvalidLogRecordCrc,
}
