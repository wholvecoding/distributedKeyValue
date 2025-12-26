package com.dkv.dkvstorage.rocksdb;
import org.rocksdb.*;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;

public class RocksDbEngine implements StorageEngine {
    private RocksDB db;
    private BloomFilter<CharSequence> bloomFilter;
    private final AtomicLong writeCount = new AtomicLong(0);
    private static final int BLOOM_FILTER_EXPECTED_INSERTIONS = 1_000_000;
    private static final double BLOOM_FILTER_FPP = 0.01;

    @Override
    public void init(String dbPath) throws Exception {
        // 初始化RocksDB配置
        RocksDB.loadLibrary();

        final Options options = new Options()
                .setCreateIfMissing(true)
                .setMaxBackgroundJobs(4)
                .setMaxOpenFiles(-1)
                .setTargetFileSizeBase(64 * 1024 * 1024)
                .setWriteBufferSize(64 * 1024 * 1024)
                .setMaxWriteBufferNumber(3);

        // 配置Column Family（可选）
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction();

        // 打开数据库
        this.db = RocksDB.open(options, dbPath);

        // 初始化BloomFilter
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                BLOOM_FILTER_EXPECTED_INSERTIONS,
                BLOOM_FILTER_FPP
        );
    }

    @Override
    public void put(String key, byte[] value) throws Exception {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        synchronized (this) {
            // 1. 写布隆过滤器
            bloomFilter.put(key);

            // 2. 写RocksDB
            db.put(key.getBytes(StandardCharsets.UTF_8), value);

            // 3. 更新计数器
            writeCount.incrementAndGet();

            // 4. 定期持久化BloomFilter（简化处理，实际应该定期或定量持久化）
            if (writeCount.get() % 10000 == 0) {
                // 可以在这里添加BloomFilter持久化逻辑
            }
        }
    }

    @Override
    public byte[] get(String key) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // 1. 查布隆过滤器（性能优化）
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // 2. 查RocksDB
        return db.get(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void delete(String key) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        synchronized (this) {
            // 注意：BloomFilter不支持删除操作
            // 我们只能标记删除，或者重建BloomFilter
            db.delete(key.getBytes(StandardCharsets.UTF_8));

            // 对于删除操作，BloomFilter可能会产生误判
            // 生产环境可以考虑使用Counting Bloom Filter或定期重建
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
        }
    }

    // 统计信息
    public long getWriteCount() {
        return writeCount.get();
    }

    public long getEstimatedSize() {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            return -1;
        }
    }
}
