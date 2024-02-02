use bytes::Bytes;

use crate::data::log_record::LogRecordPos;
use crate::options::{IndexType, IteratorOptions};

pub mod btree;

//Indexer 抽象数据接口，后续如果想要接入其他数据结构，则可以实现这个trait即可
pub trait Indexer {
	//向索引中存储key对应的数据位置信息
	fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
	//根据key取出对应的索引位置信息
	fn get(&self, keys: Vec<u8>) -> Option<LogRecordPos>;
	//Delete根据key删除对应的索引位置信息
	fn delete(&self, key: Vec<u8>) -> bool;
	fn iterator(&self, opts: &IteratorOptions) -> Box<dyn IndexIterator>;
	fn list_keys(&self) -> Vec<Bytes>;
}

//根据类型打开内存索引
pub fn new_indexer(index_type: IndexType) -> Box<dyn Indexer> {
	match index_type {
		IndexType::BTree => Box::new(btree::Btree::new()),
		IndexType::SkipList => todo!(),
		_ => panic!("unknown index type"),
	}
}

pub trait IndexIterator: Sync + Send {
	//rewind重新回到迭代器的起始点
	fn rewind(&mut self);
	//seek根据传入的key查找第一个大于(或小于)等于目标的key,根据从这个key开始遍历
	fn seek(&mut self, key: Vec<u8>);
	//next跳转到下一个key,返回None则代表迭代完成
	fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
