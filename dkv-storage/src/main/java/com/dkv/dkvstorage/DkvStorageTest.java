package com.dkv.dkvstorage;
import com.dkv.dkvstorage.rocksdb.DataNode;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rocksdb.*;



public class DkvStorageTest {
    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);
     // 主方法示例
//    public static void main(String[] args) throws Exception {
//        // 示例配置
//        String nodeId = "node-1";
//        String dataDir = "./data/node1";
//        int port = 8080;
//        boolean isPrimary = true;
//        List<String> replicaNodes = Arrays.asList("localhost:8081", "localhost:8082");
//        int replicationFactor = 3;
//
//        DataNode dataNode = new DataNode(nodeId, dataDir, port, isPrimary, replicaNodes, replicationFactor);
//
//        try {
//            dataNode.start();
//
//            // 添加关闭钩子
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                dataNode.stop();
//            }));
//
//            // 保持运行
//            Thread.currentThread().join();
//
//        } catch (Exception e) {
//            logger.error("DataNode failed", e);
//            dataNode.stop();
//        }
//    }
    public static void main(String[] args) {
        String dbPath = "./simplerocksdb";

        try {
            // 1. 加载库
            RocksDB.loadLibrary();
            System.out.println("✅ RocksDB库加载成功");

            // 2. 使用独立的Options对象
            Options options = null;
            RocksDB db = null;

            try {
                options = new Options();
                options.setCreateIfMissing(true);

                // 3. 打开数据库
                db = RocksDB.open(options, dbPath);
                System.out.println("✅ 数据库打开成功");

                // 4. 简单写入读取测试
                String testKey = "test";
                String testValue = "Hello RocksDB from simple test";

                // 写入
                db.put(testKey.getBytes(), testValue.getBytes());
                System.out.println("✅ 写入数据成功");

                // 读取
                byte[] result = db.get(testKey.getBytes());
                if (result != null) {
                    System.out.println("📖 读取结果: " + new String(result));
                }

                System.out.println("🎉 简单测试通过！");

            } finally {
                // 5. 手动关闭，确保顺序正确
                if (db != null) {
                    try {
                        db.close();
                        System.out.println("✅ 数据库关闭成功");
                    } catch (Exception e) {
                        System.err.println("⚠️ 关闭数据库时出现警告: " + e.getMessage());
                    }
                }

                if (options != null) {
                    try {
                        options.close();
                        System.out.println("✅ Options关闭成功");
                    } catch (Exception e) {
                        System.err.println("⚠️ 关闭Options时出现警告: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("❌ 测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }


}