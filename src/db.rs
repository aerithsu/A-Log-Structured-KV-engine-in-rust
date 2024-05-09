use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::batch::{log_record_key_with_seq, NON_TRANSACTION_SEQ_NO, parse_log_record_key};
use crate::data::data_file::{DATA_FILE_NAME_SUFFIX, DataFile};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType, ReadLogRecord, TransactionRecord};
use crate::errors::{Errors, Result};
use crate::index::{Indexer, new_indexer};
use crate::options::Options;

const INITIAL_FILE_ID: u32 = 0;

//使用一个叫做bytes的crate
//bitcask存储引擎实例结构
pub struct Engine {
	options: Arc<Options>,
	active_file: Arc<RwLock<DataFile>>,
	//当前活跃文件
	older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
	//旧的数据文件
	pub(crate) indexer: Box<dyn Indexer>,
	//索引接口的实现
	file_ids: Vec<u32>,
	//数据库启动时的文件id,只用于加载索引时使用,不能在其他地方更新或使用
	pub(crate) batch_commit_lock: Mutex<()>,
	//事务提交保证串行化的锁
	pub(crate) seq_no: Arc<AtomicUsize>,
	//全局事务序列号
}

//别的crate里面也有为Engine实现的方法
impl Engine {
	//打开bitcask存储引擎实例
	pub fn open(opts: Options) -> Result<Engine> {
		//对传递进来的配置项进行校验
		if let Some(e) = check_options(&opts) {
			return Err(e);
		}
		let dir_path = &opts.dir_path;
		//判断数据目录是否存在,如果不存在则创建这个目录
		if !dir_path.is_dir() {
			//目录不存在且创建目录失败
			if let Err(e) = fs::create_dir(dir_path) {
				warn!("create database directory err:{}", e);
				return Err(Errors::FailedToCreateDatabaseDir);
			}
		}
		//加载数据文件,把目录里面的文件加载为DataFile结构,按照id逆序存入一个Vec中
		let mut data_files = load_data_files(dir_path)?;
		//设置file_id信息
		let mut file_ids = vec![];
		for data_file in &data_files {
			file_ids.push(data_file.get_file_id());
		}
		//id最大的元素为active file,其他文件放入older_files HashMap中即可
		let mut older_files = HashMap::new();
		if data_files.len() > 1 {
			//弹出后面的n - 1个元素
			for _ in 0..=data_files.len() - 2 {
				let file = data_files.pop().unwrap();
				older_files.insert(file.get_file_id(), file);
			}
		}
		//如果目录里面无文件,需要创建一个数据文件,作为active file
		let active_file = match data_files.pop() {
			Some(file) => file,
			None => DataFile::new(dir_path, INITIAL_FILE_ID)?, //这代表数据库目录里面没有一个文件
		};
		//构造存储引擎实例
		let engine = Engine {
			options: Arc::new(opts.clone()),
			active_file: Arc::new(RwLock::new(active_file)),
			older_files: Arc::new(RwLock::new(older_files)),
			file_ids,
			indexer: new_indexer(opts.index_type),
			batch_commit_lock: Mutex::new(()),
			seq_no: Arc::new(AtomicUsize::new(0)),
		};
		// 从数据文件中加载索引
		let current_seq_no = engine.load_index_from_data_files()?;

		// 更新当前事务序列号
		if current_seq_no > 0 {
			engine.seq_no.store(current_seq_no, Ordering::SeqCst);
		}
		Ok(engine)
	}
	//数据写入
	pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
		//判断key的有效性
		if key.is_empty() {
			return Err(Errors::KeyIsEmpty);
		}
		//构造LogRecord
		let mut record = LogRecord {
			//直接调用put的是非事务的LogRecord,为了统一,用NO_TRANSACTION_SEQ_NO标识其key
			key: log_record_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
			value: value.to_vec(),
			rec_type: LogRecordType::NORMAL,
		};
		//将数据追加写入到当前的活跃文件中
		let log_record_pos = self.append_log_record(&mut record)?;
		//更新内存索引
		if !self.indexer.put(key.to_vec(), log_record_pos) {
			return Err(Errors::IndexUpdateFailed);
		}
		Ok(())
	}
	//追加数据到当前活跃文件中,返回写入的file_id和offset(用结构体LogRecordPos封装),用于更新内存里面的索引
	//注意当前active file容量达到最大后要把其加入old_files哈希表里,创建新的active file
	//这个方法在当前crate(lib.rs)的别的模块里面也会使用,令其可见性为pub(crate)
	pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
		let dir_path = self.options.dir_path.clone();
		//对输入的数据进行编码
		let enc_record = log_record.encode();
		let record_len = enc_record.len() as u64;
		//获取到当前活跃文件的写锁
		let mut active_file = self.active_file.write();
		//判断当前活跃文件是否到达写入的阈值
		if active_file.get_write_off() + record_len > self.options.data_file_size {
			//将当前的活跃文件进行持久化
			active_file.sync()?;
			let current_fid = active_file.get_file_id();
			//将旧的数据文件放入map中
			let mut older_files = self.older_files.write();
			let old_file = DataFile::new(&dir_path, current_fid)?;
			older_files.insert(current_fid, old_file);
			//打开新的数据文件,作为新的active file,同时其file_id为前一个active file的id + 1
			let new_file = DataFile::new(&dir_path, current_fid + 1)?;
			*active_file = new_file;
		}
		let write_off = active_file.get_write_off();
		//把编码后的LogRecord写入到当前offset处,这个方法同时更新了写入文件的offset
		active_file.write(&enc_record)?;
		//根据配置文件决定是否每次写都持久化
		if self.options.sync_writes {
			active_file.sync()?;
		}
		Ok(LogRecordPos {
			file_id: active_file.get_file_id(),
			offset: write_off,
		})
	}
	//通过LogRecordPos来找到对应的value,以Vec<u8>形式返回
	pub(crate) fn get_value_by_position(&self, pos: LogRecordPos) -> Result<Bytes> {
		let active_file = self.active_file.read();
		let older_file = self.older_files.read();
		//从对应的文件里面读出LogRecord
		let log_record = if active_file.get_file_id() == pos.file_id {
			//记录在当前活跃文件里
			active_file.read_log_record(pos.offset)?.record
		} else {
			let file = older_file.get(&pos.file_id);
			if file.is_none() {
				//找不到对应的数据文件
				return Err(Errors::DataFileNotFound);
			}
			file.unwrap().read_log_record(pos.offset)?.record
		};
		//判断LogRecord的类型
		if log_record.rec_type == LogRecordType::DELETED {
			return Err(Errors::KeyNotFound);
		}
		Ok(log_record.value.into()) //Bytes结构体有实现From<Vec<u8>>的trait
	}
	//数据读取
	pub fn get(&self, key: Bytes) -> Result<Bytes> {
		if key.is_empty() {
			return Err(Errors::KeyIsEmpty);
		}
		//找到LogRecord在的文件和对应offset
		let pos = self.indexer.get(key.to_vec());
		if pos.is_none() {
			return Err(Errors::KeyNotFound);
		}
		let pos = pos.unwrap();
		//从对应的数据文件中获取LogRecord
		// let active_file = self.active_file.read();
		// let older_file = self.older_files.read();
		self.get_value_by_position(pos)
	}
	//delete就是插入一个类型为DELETE的LogRecord,也要调用append_log_record方法
	pub fn delete(&self, key: Bytes) -> Result<()> {
		if key.is_empty() {
			return Err(Errors::KeyIsEmpty);
		}
		//从索引中取出相应的数据,如果不存在则直接返回
		let pos = self.indexer.get(key.to_vec());
		if pos.is_none() {
			return Ok(());
		}
		let mut record = LogRecord {
			key: key.to_vec(),
			value: Default::default(),
			rec_type: LogRecordType::DELETED,
		};
		self.append_log_record(&mut record)?;
		//从内存索引中删除key
		let ok = self.indexer.delete(key.to_vec());
		if !ok {
			return Err(Errors::IndexUpdateFailed);
		}
		Ok(())
	}
	pub fn sync(&self) -> Result<()> {
		//只用sync 活跃文件就好了
		self.active_file.write().sync()
	}
	pub fn close(&self) -> Result<()> {
		self.sync()
	}

	//遍历数据文件中的内容,并依次处理其中所有的记录,构建其内存索引key->LogRecordPos
	//这一步比较耗时,后面可以优化(空间换时间,用一个hint文件来存储相关信息)
	fn load_index_from_data_files(&self) -> Result<usize> {
		if self.file_ids.is_empty() {
			return Ok(NON_TRANSACTION_SEQ_NO);
		}
		//用来记录用到哪个seq_no了
		let mut current_seq_no = NON_TRANSACTION_SEQ_NO;
		let active_file = self.active_file.read();
		let older_file = self.older_files.read();
		//暂存事务相关的数据,存储对应的LogRecord和其pos
		let mut transaction_record = HashMap::new();
		//遍历所有的文件
		for (i, file_id) in self.file_ids.iter().enumerate() {
			let mut offset = 0;
			loop {
				let log_record_res = match *file_id == active_file.get_file_id() {
					true => active_file.read_log_record(offset),
					false => {
						let data_file = older_file.get(file_id).unwrap();
						data_file.read_log_record(offset)
					}
				};
				//这里是为了解构处record和size两个变量,size同名所以可以不用写字段名
				let ReadLogRecord {
					record: mut log_record,
					size: size,
				} = match log_record_res {
					Ok(result) => result,
					Err(e) => {
						//读到文件尾了,直接读取下一个文件
						if e == Errors::ReadDataFileEOF {
							break;
						}
						return Err(e);
					}
				};
				//构建内存索引
				let log_record_pos = LogRecordPos {
					file_id: *file_id,
					offset,
				};


				let (real_key, seq_no) = parse_log_record_key(&log_record.key);
				//非事务提交,直接更新其内存索引
				if seq_no == NON_TRANSACTION_SEQ_NO {
					self.update_index(real_key, log_record.rec_type, log_record_pos)?;
				} else {
					//读取到TXN_FINISHED的记录说明何其seq_no相同的记录都是有效的
					if log_record.rec_type == LogRecordType::TXN_FINISHED {
						//需要指明类型
						// dbg!(&transaction_record);
						let records: &Vec<TransactionRecord> = transaction_record.get(&seq_no).unwrap();
						for txn_record in records {
							self.update_index(txn_record.record.key.clone(), txn_record.record.rec_type, txn_record.pos)?;
						}
						transaction_record.remove(&seq_no);
					} else {
						log_record.key = real_key;
						transaction_record.entry(seq_no).or_insert(Vec::new())
							.push(TransactionRecord {
								record: log_record,
								pos: log_record_pos,
							});
					}
				}
				//更新当前序列号
				if seq_no > current_seq_no {
					current_seq_no = seq_no;
				}

				//更新offset,下次读取的时候从新的位置开始
				offset += size;
			}
			//设置活跃文件的offset
			if i == self.file_ids.len() - 1 {
				active_file.set_write_off(offset);
			}
		}
		Ok(current_seq_no)
	}
	//加载索引更新内存数据
	fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) -> Result<()> {
		//针对不同的LogRecordType操作不同
		let ok = match rec_type {
			LogRecordType::NORMAL => {
				self.indexer.put(key.to_vec(), pos)
			}
			LogRecordType::DELETED => self.indexer.delete(key.to_vec()),
			LogRecordType::TXN_FINISHED => true,
		};
		if !ok {
			return Err(Errors::IndexUpdateFailed);
		}
		Ok(())
	}
}

//先把所有数据文件的id加载入一个Vec，逆序排序，再根据这个Vec里面的file_id按序加载数据文件为DataFile
fn load_data_files(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
	let dir = fs::read_dir(dir_path);
	if dir.is_err() {
		return Err(Errors::FailedToReadDataBaseDir);
	}
	let mut file_ids = vec![];
	let mut data_files = vec![];
	for file in dir.unwrap() {
		//拿到文件名
		let entry = file.unwrap();
		let file_os_str = entry.file_name();
		let file_name = file_os_str.to_str().unwrap();
		//判断文件是不是我们对应的数据文件(以.data为后缀)
		if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
			//文件名的格式为数字+.data
			let spilt_names: Vec<&str> = file_name.split('.').collect();
			let file_id = match spilt_names[0].parse::<u32>() {
				Ok(fid) => fid,
				Err(_) => return Err(Errors::DataDirectoryCorrupted),
			};
			file_ids.push(file_id);
		}
	}
	//对文件id进行排序,这里是快速排序,且为逆序排序
	file_ids.sort_unstable_by(|a, b| b.cmp(a));
	//遍历所有的文件id,依次打开对应的数据文件(因为这是日志型数据库)
	for file_id in file_ids {
		data_files.push(DataFile::new(dir_path, file_id)?);
	}
	Ok(data_files)
}

//判断传入的打开数据库实例的Option是不是合法的,比如目录为空或者data_file_size == 0都是不合法的
fn check_options(opts: &Options) -> Option<Errors> {
	let dir_path = opts.dir_path.to_str();
	if dir_path.is_none() || dir_path.unwrap().is_empty() {
		return Some(Errors::DirPathIsEmpty);
	}
	if opts.data_file_size == 0 {
		return Some(Errors::DataFileSizeTooSmall);
	}
	None
}
