use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;

use crate::data::data_file::{DataFile, DATA_FILE_NAME_SUFFIX};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType, ReadLogRecord};
use crate::errors::Errors::KeyIsEmpty;
use crate::errors::{Errors, Result};
use crate::index::{new_indexer, Indexer};
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
    indexer: Box<dyn Indexer>,
    file_ids: Vec<u32>, //数据库启动时的文件id,只用于加载索引时使用,不能在其他地方更新或使用
}

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
            if let Err(e) = fs::create_dir(&dir_path) {
                warn!("create database directory err:{}", e);
                return Err(Errors::FailedToCreateDatabaseDir);
            }
        }
        //加载数据文件
        let mut data_files = load_data_files(&dir_path)?;
        //设置file_id信息
        let mut file_ids = vec![];
        for data_file in &data_files {
            file_ids.push(data_file.get_file_id());
        }
        //id最大的元素为active file,其他文件放入older_files HashMap中即可
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID)?, //这代表数据库目录里面没有一个文件
        };
        //构造存储引擎实例
        let engine = Engine {
            options: Arc::new(opts.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            file_ids,
            indexer: new_indexer(opts.index_type),
        };
        engine.load_index_from_data_files()?;
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
            key: key.to_vec(),
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
    //追加数据到当前活跃文件中
    //为什么要使用&mut LogRecord
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();
        //对输入的数据进行编码
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;
        //获取到当前活跃文件
        let mut active_file = self.active_file.write();
        //判断当前活跃文件是否到达写入的阈值
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            //将当前的活跃文件进行持久化
            active_file.sync()?;
            let current_fid = active_file.get_file_id();
            //将旧的数据文件放入map中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);
            //打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }
        let write_off = active_file.get_write_off();
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
    //数据读取
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let pos = self.indexer.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }
        let pos = pos.unwrap();
        //从对应的数据文件中获取LogRecord
        let active_file = self.active_file.read();
        let older_file = self.older_files.read();
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
        if log_record.rec_type == LogRecordType::DELETE {
            return Err(Errors::KeyNotFound);
        }
        Ok(log_record.value.into()) //Bytes结构体有实现From<Vec<u8>>的trait
    }
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(KeyIsEmpty);
        }
        //从索引中取出相应的数据,如果不存在则直接返回
        let pos = self.indexer.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        let mut record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETE,
        };
        self.append_log_record(&mut record)?;
        //从内存索引中删除key
        let ok = self.indexer.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }
    //从数据文件中加载内存索引
    //遍历数据文件中的内容,并依次处理其中的记录
    fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_file.read();
        let older_file = self.older_files.read();
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
                let ReadLogRecord {
                    record: log_record,
                    size,
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
                let ok = match log_record.rec_type {
                    LogRecordType::NORMAL => {
                        self.indexer.put(log_record.key.to_vec(), log_record_pos)
                    }
                    LogRecordType::DELETE => self.indexer.delete(log_record.key.to_vec()),
                };
                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }
                //更新offset,下次读取的时候从新的位置开始
                offset += size;
            }
            //设置活跃文件的offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(())
    }
}

fn load_data_files(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDataBaseDir);
    }
    let mut file_ids = vec![];
    let mut data_files = vec![];
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            //拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            //判断文件是不是我们对应的数据文件(以.data为后缀)
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                //文件名的格式为数字+.data
                let spilt_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match spilt_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => return Err(Errors::DataDirectoryCorrupted),
                };
                file_ids.push(file_id);
            }
        }
    }
    //如果没有数据文件则直接返回
    //对文件id进行排序
    file_ids.sort_by(|a, b| b.cmp(a));
    //遍历所有的文件id,一次打开对应的数据文件(因为这是日志型数据库)
    for file_id in file_ids {
        data_files.push(DataFile::new(dir_path.clone(), file_id)?);
    }
    Ok(data_files)
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }
    if opts.data_file_size == 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }
    None
}
