use bytes::Bytes;

#[allow(dead_code)]
pub fn get_test_key(i: i32) -> Bytes {
    Bytes::from(format!("bitcask-key-{:09}", i))
}

//get_test_value 方法增加一点长度，不然测不到文件写满转换的 case
#[allow(dead_code)]
pub fn get_test_value(i: i32) -> Bytes {
    Bytes::from(format!(
        "bitcask-rs-value-value-value-value-value-value-value-value-value-{:09}",
        i
    ))
}

#[test]
fn test_get_test_key() {
    assert_eq!(
        Bytes::from(format!("bitcask-key-{:09}", 1)),
        get_test_key(1)
    );
    assert_eq!(
        Bytes::from(format!("bitcask-value-{:09}", 3)),
        get_test_value(3)
    );
    for i in 0..=10 {
        assert!(!get_test_key(i).is_empty());
        assert!(!get_test_value(i).is_empty());
    }
}
