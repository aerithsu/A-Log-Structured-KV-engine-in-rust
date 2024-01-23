use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

//对标准库BtreeMap简单封装
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
        let mut read_guard = self.tree.read();
        read_guard.get(&key).cloned()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut writer_guard = self.tree.write();
        writer_guard.remove(&key).is_some()
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
}
