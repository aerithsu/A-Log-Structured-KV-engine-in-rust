use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

// use crate::data::log_record::LogRecordType::TXN_FINISHED;
use crate::data::log_record::{LogRecord, LogRecordType};
use crate::data::log_record::LogRecordType::TXN_FINISHED;
use crate::db::Engine;
use crate::errors::{Errors, Result};
use crate::options::WriteBatchOptions;

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
//用来标识非事务(即非批量写入的key),批量写入的key其seq_no从1开始
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;

//批量写数据,保证原子性
pub struct WriteBatch<'a> {
	//使用hashmap对比数组的优点为可以去除重复的key
	pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
	//暂存用户写入的数据
	engine: &'a Engine,//engine生命周期>=write_batch
	options: WriteBatchOptions,
}

impl Engine {
	pub fn new_write_batch(&self, write_batch_options: WriteBatchOptions) -> WriteBatch {
		WriteBatch {
			pending_writes: Arc::new(Mutex::new(HashMap::new())),
			engine: self,
			options: write_batch_options,
		}
	}
}

impl WriteBatch<'_> {
	//将要写入的数据暂存在一个哈希表里面,等commit时再批量写道数据文件里面
	pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
		if key.is_empty() {
			return Err(Errors::KeyIsEmpty);
		}
		let record = LogRecord {
			key: key.to_vec(),
			value: value.to_vec(),
			rec_type: LogRecordType::NORMAL,
		};
		//暂存数据
		let mut pending_writes = self.pending_writes.lock();
		pending_writes.insert(key.to_vec(), record);
		Ok(())
	}
	pub fn delete(&self, key: Bytes) -> Result<()> {
		if key.is_empty() {
			return Err(Errors::KeyIsEmpty);
		}
		let mut pending_writes = self.pending_writes.lock();
		if self.engine.indexer.get(key.to_vec()).is_none() {
			//虽然key可能在数据库中不存在,但是可能存在于batch中,直接在暂存的数据里面删除即可
			if pending_writes.contains_key(&key.to_vec()) {
				pending_writes.remove(&key.to_vec());
			}
			return Ok(());
		}
		let record = LogRecord {
			key: key.to_vec(),
			value: vec![],
			rec_type: LogRecordType::DELETED,
		};
		pending_writes.insert(key.to_vec(), record);
		Ok(())
	}
	//提交数据,将数据写到文件中,并更新内存索引
	pub fn commit(&self) -> Result<()> {
		let mut pending_writes = self.pending_writes.lock();
		if pending_writes.len() == 0 {
			return Ok(());
		}
		//一次写入的批次不能太大,防止内存用掉太多
		if pending_writes.len() > self.options.max_batch_num {
			return Err(Errors::ExceedMaxBatchNum);
		}
		//获取全局锁,加锁保证串行化
		let _lock = self.engine.batch_commit_lock.lock();
		//获取全局的事务序列号
		//这个方法给原子类型+1并返回旧的值
		let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst) + 1; //得到序列号后在递增

		//最后要统一更新的内存索引,先暂存在一个哈希表里面 
		let mut positions = HashMap::new();
		//写数据到数据文件中
		for (_, item) in pending_writes.iter() {
			let mut record = LogRecord {
				key: log_record_key_with_seq(item.key.clone(), seq_no),
				value: item.key.clone(),
				rec_type: item.rec_type,
			};
			let pos = self.engine.append_log_record(&mut record)?;
			positions.insert(item.key.clone(), pos);
		}
		//写最后一条标识事务完成的数据
		let mut finish_record = LogRecord {
			key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
			value: vec![],
			rec_type: TXN_FINISHED,
		};
		self.engine.append_log_record(&mut finish_record)?;
		//将数据持久化
		if self.options.sync_writes {
			self.engine.sync()?;
		}
		//执行到这里说明前面的数据都已经写入到了DataFile里面
		//数据全部写完之后再更新内存索引
		for (_, item) in pending_writes.iter() {
			let record_pos = positions.get(&item.key).unwrap();
			if item.rec_type == LogRecordType::NORMAL {
				self.engine.indexer.put(item.key.clone(), *record_pos);
			} else if item.rec_type == LogRecordType::DELETED {
				self.engine.indexer.delete(item.key.clone());
			}
		}
		//清空暂存数据,防止其影响下一次的批量提交
		pending_writes.clear();
		Ok(())
	}
}

//编码seq_no和key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
	let mut enc_key = BytesMut::new();
	encode_length_delimiter(seq_no, &mut enc_key).unwrap();
	enc_key.extend_from_slice(&key.to_vec());
	enc_key.to_vec()
}

//由于把seq_no和key编码到一起了,那么更新索引的时候需要把seq_no和key分开
pub(crate) fn parse_log_record_key(key: &[u8]) -> (Vec<u8>, usize) {
	let mut buf = BytesMut::new();
	buf.put_slice(key);
	//这里传入&mut buf的原因是因为这个方法会修改里面的cursor
	let seq_no = decode_length_delimiter(&mut buf).unwrap();
	(buf.to_vec(), seq_no)
}

#[cfg(test)]
mod test {
	use std::path::PathBuf;

	use crate::options::Options;
	use crate::util;

	use super::*;

	#[test]
	fn test_write_batch_1() {
		let mut opts = Options::default();
		opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
		opts.data_file_size = 64 * 1024 * 1024;
		let engine = Engine::open(opts.clone()).expect("failed to open engine");

		let wb = engine
			.new_write_batch(WriteBatchOptions::default());
		// 写数据之后未提交
		let put_res1 = wb.put(
			util::rand_kv::get_test_key(1),
			util::rand_kv::get_test_value(10),
		);
		assert!(put_res1.is_ok());
		let put_res2 = wb.put(
			util::rand_kv::get_test_key(2),
			util::rand_kv::get_test_value(10),
		);
		assert!(put_res2.is_ok());

		let res1 = engine.get(util::rand_kv::get_test_key(1));
		assert_eq!(Errors::KeyNotFound, res1.err().unwrap());

		// 事务提交之后进行查询
		let commit_res = wb.commit();
		assert!(commit_res.is_ok());

		let res2 = engine.get(util::rand_kv::get_test_key(1));
		assert!(res2.is_ok());

		// 验证事务序列号
		let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
		assert_eq!(1, seq_no);

		// 删除测试的文件夹
		std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
	}

	#[test]
	fn test_write_batch_2() {
		let mut opts = Options::default();
		opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
		opts.data_file_size = 64 * 1024 * 1024;
		let engine = Engine::open(opts.clone()).expect("failed to open engine");

		let wb = engine
			.new_write_batch(WriteBatchOptions::default());
		let put_res1 = wb.put(
			util::rand_kv::get_test_key(1),
			util::rand_kv::get_test_value(10),
		);
		assert!(put_res1.is_ok());
		let put_res2 = wb.put(
			util::rand_kv::get_test_key(2),
			util::rand_kv::get_test_value(10),
		);
		assert!(put_res2.is_ok());
		let commit_res1 = wb.commit();
		assert!(commit_res1.is_ok());

		let put_res3 = wb.put(
			util::rand_kv::get_test_key(1),
			util::rand_kv::get_test_value(10),
		);
		assert!(put_res3.is_ok());

		let commit_res2 = wb.commit();
		assert!(commit_res2.is_ok());
		let seq_no = engine.seq_no.load(Ordering::SeqCst);
		assert_eq!(2, seq_no);
		// 重启之后进行校验
		engine.close().expect("failed to close");
		std::mem::drop(engine);

		let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
		let keys = engine2.list_keys();
		assert_eq!(2, keys.len());

		// 验证事务序列号
		let seq_no = engine2.seq_no.load(Ordering::SeqCst);
		assert_eq!(2, seq_no);

		// 删除测试的文件夹
		std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
	}
}