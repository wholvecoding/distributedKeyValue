package com.dkv.dkvstorage.rocksdb;

public interface StorageEngine {
    void init(String dbPath) throws Exception;
    void put(String key, byte[] value) throws Exception;
    byte[] get(String key) throws Exception;
    void delete(String key) throws Exception;
    void close();
}


