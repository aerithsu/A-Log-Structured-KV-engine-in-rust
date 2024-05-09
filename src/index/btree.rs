use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;
use crate::errors::Result;
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;

//对标准库BtreeMap简单封装,用读写锁和Arc来包装,读写锁保证了线程安全,Arc可以让多个线程拥有其所有权
pub struct Btree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Btree {
    pub fn new() -> Btree {
        Btree {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

//为什么使用&self而不是&mut self呢?
impl Indexer for Btree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        //这里利用了内部可变性
        let mut writer_guard = self.tree.write();
        writer_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).cloned()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut writer_guard = self.tree.write();
        writer_guard.remove(&key).is_some()
    }
    //为Btree创建迭代器可能会导致内存膨胀
    //这里迭代器的实现比较简单粗暴,把所有的key和对应的LogRecordPos加入到一个Vec里面,维护这个Vec的index
    fn iterator(&self, opts: &IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        //将Btree的数据存在数组中
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), value.clone()));
        }
        if opts.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options: opts.clone(),
        })
    }
    //返回装有所有key的Vec
    fn list_keys(&self) -> Vec<Bytes> {
        let read_guard = self.tree.read();
        let mut keys = vec![];
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(k));
        }
        keys
    }
}

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    //这是有序的
    curr_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0
    }
    fn seek(&mut self, key: Vec<u8>) {
        //二分查找key所在的位置
        let res = self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        });
        //这个函数如果找到返回OK(pos),否则返回Err(pos)(这里pos为要插入的位置)
        self.curr_index = res.unwrap_or_else(|pos| pos);
    }
    //iterator向前移动一位
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            //用户没有指定prefix或者我们已经找到了以prefix开头的数据,则可以返回
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }
        //遍历结束都没找到满足prefix的数据
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_btree_put() {
        let mut bt = Btree::new();
        //&str类型的as_bytes得到&[u8],调用to_vec()方法得到Vec<u8>
        let res = bt.put(
            "vec![1,2]".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        assert_eq!(res, true);
        let res = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res, true);
        let res = bt.put(
            "aaa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        assert_eq!(res, true);
        // println!("{:#?}",bt.get("aa".as_bytes().to_vec()));
    }

    #[test]
    fn test_btree_get() {
        let mut bt = Btree::new();
        bt.put(
            "vec![1,2]".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        let res = bt.get("vec![1,2]".as_bytes().to_vec());
        assert!(res.is_some());
        assert!(res.unwrap().file_id == 1 && res.unwrap().offset == 2);
    }

    #[test]
    fn test_btree_delete() {
        let mut bt = Btree::new();
        bt.put(
            "vec![1,2]".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        assert_eq!(bt.delete("vec![1,2]".as_bytes().to_vec()), true);
        assert_eq!(bt.delete("vec![1,2]".as_bytes().to_vec()), false);
    }

    #[test]
    fn test_btree_iterator_seek() {
        let bt = Btree::new();
        let mut iter1 = bt.iterator(&IteratorOptions::default());
        iter1.seek("aaa".as_bytes().to_vec());
        let res = iter1.next();
        assert!(res.is_none());
        //有一条数据的情况
        bt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter2 = bt.iterator(&IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res = iter2.next();
        assert!(res.is_some());
        //seek一条不存在的数据
        let mut iter3 = bt.iterator(&IteratorOptions::default());
        iter3.seek("zz".as_bytes().to_vec());
        assert!(iter3.next().is_none());
        //有多条数据的情况
        bt.put(
            "ba".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        bt.put(
            "aawe".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        bt.put(
            "cdsa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        let mut iter = bt.iterator(&IteratorOptions::default());
        iter.seek("b".as_bytes().to_vec());
        //没有输出以a开头的数据
        while let Some(item) = iter.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
        let mut iter = bt.iterator(&IteratorOptions::default());
        iter.seek("aawe".as_bytes().to_vec());
        assert_eq!(iter.next().unwrap().0.to_vec(), "aawe".as_bytes().to_vec());
        let mut iter = bt.iterator(&IteratorOptions::default());
        //反向迭代

        let mut iter = bt.iterator(&IteratorOptions {
            prefix: vec![],
            reverse: true,
        });
        iter.seek("cdsa".as_bytes().to_vec());
        while let Some(item) = iter.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }


    #[test]
    fn test_btree_iterator_next() {
        let bt = Btree::new();
        let mut iter1 = bt.iterator(&IteratorOptions::default());
        assert!(iter1.next().is_none());

        // 有一条数据的情况
        bt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter_opt1 = IteratorOptions::default();
        iter_opt1.reverse = true;
        let mut iter2 = bt.iterator(&iter_opt1);
        assert!(iter2.next().is_some());

        // 有多条数据的情况
        bt.put(
            "bbed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cdea".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter_opt2 = IteratorOptions::default();
        iter_opt2.reverse = true;
        let mut iter3 = bt.iterator(&iter_opt2);
        while let Some(item) = iter3.next() {
            assert!(item.0.len() > 0);
        }

        // 有前缀的情况
        let mut iter_opt3 = IteratorOptions::default();
        iter_opt3.prefix = "bbed".as_bytes().to_vec();
        let mut iter4 = bt.iterator(&iter_opt3);
        while let Some(item) = iter4.next() {
            assert!(item.0.len() > 0);
        }
    }

}
