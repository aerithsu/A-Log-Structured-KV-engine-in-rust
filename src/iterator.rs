use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::db::Engine;
use crate::errors::Result;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;
//'a 表示 engine 的引用至少与 Iterator 实例有相同的生命周期。
pub struct Iterator<'a> {
	index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
	engine: &'a Engine,
}

impl Engine {
	//获取迭代器
	pub fn iter(&self, iterator_options: &IteratorOptions) -> Iterator {
		Iterator {
			index_iter: Arc::new(RwLock::new(self.indexer.iterator(iterator_options))),
			engine: self,
		}
	}
	//返回数据库中所有的key
	pub fn list_keys(&self) -> Vec<Bytes> {
		self.indexer.list_keys()
	}
	//对数据库当中的所有数据进行函数操作,如果函数返回false则终止
	pub fn fold<F>(&self, f: F) -> Result<()>
		where
			Self: Sized,
			F: Fn(Bytes, Bytes) -> bool,
	{
		let iter = self.iter(&IteratorOptions::default());
		while let Some((key, value)) = iter.next() {
			if !f(key, value) {
				break;
			}
		}
		Ok(())
	}
}
//Fn 捕获不可变引用 FnMut捕获可变引用 FnOnce获取所有权只能调用一次

//编译器推导生命周期
impl Iterator<'_> {
	pub fn rewind(&self) {
		let mut index_iter = self.index_iter.write();
		index_iter.rewind();
	}
	pub fn seek(&self, key: Vec<u8>) {
		let mut index_iter = self.index_iter.write();
		index_iter.seek(key);
	}
	pub fn next(&self) -> Option<(Bytes, Bytes)> {
		let mut index_iter = self.index_iter.write();
		if let Some(item) = index_iter.next() {
			let value = self
				.engine
				.get_value_by_position(item.1.clone())
				.expect("failed to get value from data file");
			return Some((Bytes::from(item.0.to_owned()), value));
		}
		None
	}
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use crate::options::{IndexType, Options};
    use crate::util;

    use super::*;

    #[test]
	fn test_iterator_seek() {
		let opts = Options {
			dir_path: PathBuf::from("/tmp/bitcask-rs-iter-seek"),
			data_file_size: 256 * 1024 * 1024,
			sync_writes: false,
			index_type: IndexType::BTree,
		};

		let engine = Engine::open(opts).expect("failed to open engine");
		let iter = engine.iter(&IteratorOptions::default());
		iter.seek("aa".as_bytes().to_vec());
		assert!(iter.next().is_none());

		engine
			.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10))
			.expect("Engine failed to put");
		let iter = engine.iter(&IteratorOptions::default());
		iter.seek("a".as_bytes().to_vec());
		let res = iter.next();
		assert!(res.is_some());
		println!("{:?}", res.unwrap());

		engine
			.put(Bytes::from("abbb"), util::rand_kv::get_test_value(10))
			.expect("Engine failed to put");
		engine
			.put(Bytes::from("accc"), util::rand_kv::get_test_value(20))
			.expect("Engine failed to put");
		engine
			.put(Bytes::from("abbb"), util::rand_kv::get_test_value(30))
			.expect("Engine failed to put");
		let iter = engine.iter(&IteratorOptions::default());
		iter.seek("a".as_bytes().to_vec());
		while let Some((key, value)) = iter.next() {
			println!("{:?}:{:?}", key, value);
		}
		//删除临时文件
		fs::remove_dir_all(PathBuf::from("/tmp/bitcask-rs-iter-seek"))
			.expect("failed to remove the dir");
	}

	#[test]
	fn test_iterator_next() {
		let mut opts = Options {
			dir_path: PathBuf::from("/tmp/bitcask-rs-iter-seek"),
			data_file_size: 256 * 1024 * 1024,
			sync_writes: false,
			index_type: IndexType::BTree,
		};

		let engine = Engine::open(opts).expect("failed to open engine");
		engine
			.put(Bytes::from("accc"), util::rand_kv::get_test_value(20))
			.expect("failed to put");
		let iter = engine.iter(&IteratorOptions::default()); //每一个iter都是一个数据库里面数据的副本
		assert!(iter.next().is_some());
		assert!(iter.next().is_none());
		iter.rewind();
		assert!(iter.next().is_some());
		assert!(iter.next().is_none());
		fs::remove_dir_all(PathBuf::from("/tmp/bitcask-rs-iter-seek"))
			.expect("failed to remove the dir");
	}
}
