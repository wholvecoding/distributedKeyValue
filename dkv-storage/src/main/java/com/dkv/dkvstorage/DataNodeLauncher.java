package com.dkv.dkvstorage;

import com.dkv.dkvstorage.rocksdb.DataNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//public class DataNodeLauncher {
//
//    public static void main(String[] args) throws Exception {
//
//        String nodeId = System.getProperty("nodeId");
//        System.out.println(nodeId);
//        String dataDir = System.getProperty("dataDir");
//        System.out.println(dataDir);
//        int port = Integer.parseInt(System.getProperty("port"));
//        System.out.println(port);
//        boolean isPrimary = Boolean.parseBoolean(System.getProperty("isPrimary"));
//        System.out.println(isPrimary);
//        String replicas = System.getProperty("replicas", "");
//        System.out.println(replicas);
//        List<String> replicaNodes = replicas.isEmpty()
//                ? List.of()
//                : Arrays.asList(replicas.split(","));
//
//        int replicationFactor = replicaNodes.size() + 1;
//
//        DataNode node = new DataNode(
//                nodeId,
//                dataDir,
//                port,
//                isPrimary,
//                replicaNodes,
//                replicationFactor
//        );
//
//        node.start();
//        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
//    }
//}

public class DataNodeLauncher {

    public static void main(String[] args) throws Exception {
        // 1. 定义节点配置（每个节点包含：ID, 端口, 是否为主节点）
        // 这里模拟启动 3 个本地节点
        NodeConfig[] configs = {
                new NodeConfig("127.0.0.1", 9001, true),
                new NodeConfig("127.0.0.1", 9002, false),
                new NodeConfig("127.0.0.1", 9003, false)
        };

        List<DataNode> runningNodes = new ArrayList<>();

        for (NodeConfig config : configs) {
            // 2. 为每个节点分配独立的数据存放路径（非常重要！）
            String dataDir = "F:\\javaProject\\distributedKeyValue\\data\\" + config.id+"_"+config.port;

            // 3. 计算副本列表（简单逻辑：除了自己以外的所有节点）
            List<String> replicaNodes = new ArrayList<>();
            for (NodeConfig c : configs) {
                if (c.port != config.port) {
                    replicaNodes.add("127.0.0.1:" + c.port);
                }
            }

            // 4. 创建 DataNode 实例
            DataNode node = new DataNode(
                    config.id,
                    dataDir,
                    config.port,
                    config.isPrimary,
                    replicaNodes,
                    configs.length // 复制因子即总节点数
            );

            // 5. 在独立的线程中启动，防止阻塞后续节点启动
            new Thread(() -> {
                try {
                    node.start();
                    System.out.println(">>> 节点 [" + config.id + "] 启动成功，端口: " + config.port);
                } catch (Exception e) {
                    System.err.println(">>> 节点 [" + config.id +":"+ config.port+ "] 启动失败: " + e.getMessage());
                }
            }).start();

            runningNodes.add(node);
        }

        // 6. 注册关闭钩子，优雅关闭所有 RocksDB 实例
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println(">>> 正在关闭所有 DataNode...");
            for (DataNode node : runningNodes) {
                node.stop();
            }
        }));
    }

    // 一个简单的内部类来承载配置
    static class NodeConfig {
        String id;
        int port;
        boolean isPrimary;

        NodeConfig(String id, int port, boolean isPrimary) {
            this.id = id;
            this.port = port;
            this.isPrimary = isPrimary;
        }
    }
}

