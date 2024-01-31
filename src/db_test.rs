use std::path::PathBuf;
use bytes::Bytes;
use crate::db::Engine;
use crate::errors::Errors;
use crate::options::{IndexType, Options};
use crate::util::rand_kv::{get_test_key, get_test_value};

#[test]
fn test_engine_put_and_get() {
    let opts = Options {
        dir_path: PathBuf::from("/tmp/bitcask-rs-put"),
        data_file_size: 64 * 1024 * 2014,
        sync_writes: false,
        index_type: IndexType::BTree,
    };
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    //1.正常put一条数据
    let (key, value) = (get_test_key(11), get_test_value(11));
    let res = engine.put(key.clone(), value.clone());
    assert!(res.is_ok());
    let res = engine.get(key.clone());
    assert!(res.is_ok());
    assert!(!res.unwrap().is_empty());

    //2.重复put key相同的数据
    let res = engine.put(get_test_key(22), get_test_value(22));
    assert!(res.is_ok());
    let res = engine.put(get_test_key(22), Bytes::from("a new value"));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(22));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), Bytes::from("a new value"));

    //3.key为空
    let res = engine.put(Bytes::new(), get_test_value(1));
    assert!(res.is_err());
    assert_eq!(res, Err(Errors::KeyIsEmpty));

    //4.value为空
    let res = engine.put(get_test_key(32), Bytes::new());
    assert!(res.is_ok());
    assert!(engine.get(get_test_key(32)).unwrap().is_empty());

    //5.写到数据文件进行了转换
    for i in 0..1000000 {
        let res = engine.put(get_test_key(i), get_test_value(i));
        assert!(res.is_ok());
    }
    //todo重启后再Put数据

    //删除测试所用的文件夹
    std::fs::remove_dir_all(&opts.dir_path).expect("failed to remove path");
}

#[test]
fn test_engine_delete() {
    let opts = Options {
        dir_path: PathBuf::from("/tmp/bitcask-rs-put"),
        data_file_size: 64 * 1024 * 2014,
        sync_writes: false,
        index_type: IndexType::BTree,
    };
    let engine = Engine::open(opts.clone());

}