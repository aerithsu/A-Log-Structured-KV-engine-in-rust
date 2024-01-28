use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::error;
use parking_lot::RwLock;

use crate::errors::{Errors, Result};
use crate::fio::IOManager;

pub struct FileIO {
	fd: Arc<RwLock<File>>,
}

//数据文件(DataFile)调用实现了IOManager的结构体的相关方法进行IO
impl FileIO {
	//文件名称的路径
	pub fn new(file_name: &Path) -> Result<Self> {
		match OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.append(true)
			.open(file_name)
		{
			Ok(file) => Ok(FileIO {
				fd: Arc::new(RwLock::new(file)),
			}),
			Err(e) => {
				error!("write to data file error:{e}");
				Err(Errors::FailedToOpenDataFile)
			}
		}
	}
}

impl IOManager for FileIO {
	fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
		let read_guard = self.fd.read();
		let n_bytes = match read_guard.read_at(buf, offset) {
			Ok(n) => n,
			Err(e) => {
				error!("read from data file err:{}", e);
				return Err(Errors::FailedToReadFromDataFile);
			}
		};
		Ok(n_bytes)
	}

	fn write(&self, buf: &[u8]) -> Result<usize> {
		let mut write_guard = self.fd.write();
		match write_guard.write(buf) {
			Ok(n) => Ok(n),
			Err(e) => {
				error!("write to data file err:{}", e);
				Err(Errors::FailedToWriteToDataFile)
			}
		}
	}

	fn sync(&self) -> Result<()> {
		//为什么这里是读锁而不是写锁呢
		//读写都可以,读了之后就不能写了,但是可以有更多读的.使用写锁能更好
		let read_guard = self.fd.write();
		if let Err(e) = read_guard.sync_all() {
			error!("failed to syn data file:{}", e);
			return Err(Errors::FailedSynDataFile);
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use std::fs;

	use super::*;

	#[test]
	fn test_file_to_write() {
		let path = PathBuf::from("/tmp/a.data");
		let fio_res = FileIO::new(&path);
		assert!(fio_res.is_ok());
		let fio = fio_res.ok().unwrap();

		let res1 = fio.write("key-a\n".as_bytes());
		assert!(res1.is_ok());
		assert_eq!(6, res1.unwrap());
		let res3 = fs::remove_file(path.clone());
		assert!(res3.is_ok()); //记住测试完成后删除测试生成的文件
	}

	#[test]
	fn test_file_io_read() {
		let path = PathBuf::from("/tmp/a.data1");
		let fio_res = FileIO::new(&path);
		assert!(fio_res.is_ok());
		let fio = fio_res.ok().unwrap();

		let res1 = fio.write("key-a\n".as_bytes());
		assert!(res1.is_ok());
		assert_eq!(6, res1.unwrap());
		let mut buf = [0u8; 6];
		let read_res1 = fio.read(&mut buf, 0);
		assert!(read_res1.is_ok());
		assert_eq!(6, read_res1.unwrap());
		println!("{}", String::from_utf8(buf.to_vec()).unwrap());
		let res3 = fs::remove_file(path.clone());
		assert!(res3.is_ok()); //记住测试完成后删除测试生成的文件
	}

	#[test]
	fn test_file_io_sync() {
		let path = PathBuf::from("/tmp/a.data2"); //使用不同的文件名，因为每个测试是并发的，防止冲突
		let fio_res = FileIO::new(&path);
		assert!(fio_res.is_ok());
		let fio = fio_res.ok().unwrap();

		let res1 = fio.write("key-a\n".as_bytes());
		assert!(res1.is_ok());
		let res2 = fio.sync();
		assert!(res2.is_ok());
		let res3 = fs::remove_file(path.clone());
		assert!(res3.is_ok()); //记住测试完成后删除测试生成的文件
	}
}
