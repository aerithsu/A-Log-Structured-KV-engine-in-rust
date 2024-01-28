use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::data::log_record::{
	LogRecord, LogRecordType, max_log_record_header_size, ReadLogRecord,
};
use crate::errors::{Errors, Result};
use crate::fio::{IOManager, new_io_manager};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
	file_id: Arc<RwLock<u32>>,
	//数据文件id
	write_off: Arc<RwLock<u64>>,
	//当前写偏移,记录该数据文件写到哪个位置了
	io_manager: Box<dyn IOManager>,
}

impl DataFile {
	pub fn new(dir_path: &Path, file_id: u32) -> Result<DataFile> {
		//根据path和id构造出完整的文件名称
		let file_name = get_data_file_name(dir_path, file_id);
		//初始化io_manager
		let io_manager = new_io_manager(file_name)?;
		Ok({
			DataFile {
				file_id: Arc::new(RwLock::new(file_id)),
				write_off: Arc::new(RwLock::new(0)),
				io_manager,
			}
		})
	}
	pub fn write(&self, buf: &[u8]) -> Result<usize> {
		let n_bytes = self.io_manager.write(buf)?;
		let mut wg = self.write_off.write();
		*wg += n_bytes as u64;
		Ok(n_bytes)
	}
	pub fn set_write_off(&self, offset: u64) {
		let mut write_guard = self.write_off.write();
		*write_guard = offset;
	}
	pub fn get_write_off(&self) -> u64 {
		let read_guard = self.write_off.read();
		*read_guard
	}
	pub fn get_file_id(&self) -> u32 {
		let read_guard = self.file_id.read();
		*read_guard
	}
	pub fn sync(&self) -> Result<()> {
		self.io_manager.sync()
	}
	pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
		//先读取出header部分的数据
		let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
		self.io_manager.read(&mut header_buf, offset)?;
		//取出type,把crc放在了最后一个字节,type在第一个字节
		let rec_type = header_buf.get_u8();
		//取出key和value的长度
		let key_size = decode_length_delimiter(&mut header_buf).unwrap();
		let value_size = decode_length_delimiter(&mut header_buf).unwrap();
		//如果key和value的长度都为0,则说明读取到了文件的末尾,直接返回
		if key_size == 0 && value_size == 0 {
			return Err(Errors::ReadDataFileEOF);
		}

		//根据key和value的size读取实际的key和value

		//获取实际的header大小,type 1字节,加上key和value的size编码后的长度
		let actual_header_size =
			length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
		let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4); //最后4字节为CRC校验值
		self.io_manager
			.read(&mut kv_buf, offset + actual_header_size as u64)?;
		//构造LogRecord
		let log_record = LogRecord {
			key: kv_buf.get(..key_size).unwrap().to_vec(),
			value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
			rec_type: LogRecordType::from_u8(rec_type),
		};
		//得到CRC的值
		kv_buf.advance(key_size + value_size);
		if kv_buf.get_u32() != log_record.get_crc() {
			return Err(Errors::InvalidLogRecordCrc);
		}
		Ok(ReadLogRecord {
			record: log_record,
			size: (actual_header_size + key_size + value_size + 4) as u64,
		})
	}
}

fn get_data_file_name(dir_path: &Path, file_id: u32) -> PathBuf {
	let name = std::format!("{:09}{}", file_id, DATA_FILE_NAME_SUFFIX);
	dir_path.to_path_buf().join(name)
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_new_data_file() {
		let dir_path = std::env::temp_dir();
		let data_file = DataFile::new(&dir_path, 0);
		assert!(data_file.is_ok());
		let data_file = data_file.unwrap();
		assert_eq!(data_file.get_file_id(), 0);
		println!("temp dir:{}", dir_path.clone().display());

		let data_file = DataFile::new(&dir_path, 0);
		assert!(data_file.is_ok());
		let data_file = data_file.unwrap();
		assert_eq!(data_file.get_file_id(), 0);

		let data_file = DataFile::new(&dir_path, 3);
		assert!(data_file.is_ok());
		let data_file = data_file.unwrap();
		assert_eq!(data_file.get_file_id(), 3);
	}

	#[test]
	fn test_data_file_write() {
		let dir_path = std::env::temp_dir();
		let data_file_res = DataFile::new(&dir_path, 100);
		assert!(data_file_res.is_ok());
		let data_file = data_file_res.unwrap();
		assert_eq!(data_file.get_file_id(), 100);

		let write_res = data_file.write("aaa".as_bytes());
		assert!(write_res.is_ok());
		let write_res = write_res.unwrap();
		assert_eq!(write_res, 3);
		let write_res = data_file.write("aaaa".as_bytes());
		assert!(write_res.is_ok());
		let write_res = write_res.unwrap();
		assert_eq!(write_res, 4);
	}

	#[test]
	fn test_data_file_sync() {
		let dir_path = std::env::temp_dir();
		let data_file_res = DataFile::new(&dir_path, 200);
		assert!(data_file_res.is_ok());
		let data_file = data_file_res.unwrap();
		assert_eq!(data_file.get_file_id(), 200);

		let sync_res = data_file.sync();
		assert!(sync_res.is_ok());
	}

	#[test]
	fn test_data_file_read_log_record() {
		let dir_path = std::env::temp_dir();
		let data_file_res1 = DataFile::new(&dir_path, 700);
		assert!(data_file_res1.is_ok());
		let data_file1 = data_file_res1.unwrap();
		assert_eq!(data_file1.get_file_id(), 700);

		let enc1 = LogRecord {
			key: "name".as_bytes().to_vec(),
			value: "bitcask-rs-kv".as_bytes().to_vec(),
			rec_type: LogRecordType::NORMAL,
		};
		let write_res1 = data_file1.write(&enc1.encode());
		assert!(write_res1.is_ok());

		// 从起始位置读取
		let read_res1 = data_file1.read_log_record(0);
		assert!(read_res1.is_ok());
		let read_enc1 = read_res1.ok().unwrap().record;
		assert_eq!(enc1.key, read_enc1.key);
		assert_eq!(enc1.value, read_enc1.value);
		assert_eq!(enc1.rec_type, read_enc1.rec_type);

		// 从新的位置开启读取
		let enc2 = LogRecord {
			key: "name".as_bytes().to_vec(),
			value: "new-value".as_bytes().to_vec(),
			rec_type: LogRecordType::NORMAL,
		};
		let write_res2 = data_file1.write(&enc2.encode());
		assert!(write_res2.is_ok());

		let read_res2 = data_file1.read_log_record(24);
		assert!(read_res2.is_ok());
		let read_enc2 = read_res2.ok().unwrap().record;
		assert_eq!(enc2.key, read_enc2.key);
		assert_eq!(enc2.value, read_enc2.value);
		assert_eq!(enc2.rec_type, read_enc2.rec_type);

		// 类型是 Deleted
		let enc3 = LogRecord {
			key: "name".as_bytes().to_vec(),
			value: Default::default(),
			rec_type: LogRecordType::DELETED,
		};
		let write_res3 = data_file1.write(&enc3.encode());
		assert!(write_res3.is_ok());

		let read_res3 = data_file1.read_log_record(44);
		assert!(read_res3.is_ok());
		let read_enc3 = read_res3.ok().unwrap().record;
		assert_eq!(enc3.key, read_enc3.key);
		assert_eq!(enc3.value, read_enc3.value);
		assert_eq!(enc3.rec_type, read_enc3.rec_type);
	}
}
