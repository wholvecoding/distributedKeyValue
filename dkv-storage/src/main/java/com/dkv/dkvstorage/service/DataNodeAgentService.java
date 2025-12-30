package com.dkv.dkvstorage.service;

import com.dkv.dkvstorage.rocksdb.DataNode; // 假设这是你的 RocksDB 包装类
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy; // Spring Boot 3.x 使用 jakarta, 2.x 使用 javax

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

@Service
public class DataNodeAgentService {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeAgentService.class);

    // 管理本机运行的所有 DataNode 实例
    private final Map<String, DataNode> runningNodes = new ConcurrentHashMap<>();

    // 专用线程池用于异步启动节点，避免阻塞 HTTP 请求线程
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * 启动节点
     */
    // 在类成员变量里增加一个线程安全的 Set，用来记录正在启动中的节点
    private final Set<String> startingNodes = ConcurrentHashMap.newKeySet();

    // 引入必要的并发包

    // 假设在类成员变量里定义了锁对象
    private final Object startLock = new Object();

    public Map<String, Object> startNode(String nodeId, String dataDir, int port, boolean isPrimary, String replicasStr) {

        // 1. 解析副本配置
        List<String> replicaNodes = new ArrayList<>();
        if (replicasStr != null && !replicasStr.isEmpty()) {
            replicaNodes.addAll(Arrays.asList(replicasStr.split(",")));
        }
        int replicationFactor = replicaNodes.size() + 1;

        // 2. 加锁：确保同一时间只有一个线程能对同一个 NodeID 进行状态变更
        // 注意：这里简单起见用全局锁，更精细可以用 synchronized(nodeId.intern()) 但要注意常量池溢出风险，或者使用 ConcurrentHashMap.computeIfAbsent
        synchronized (startLock) {
            // 双重检查
            if (runningNodes.containsKey(nodeId)) {
                return Map.of("success", false, "error", "Node " + nodeId + " is already running");
            }
            // 同步模式下其实不需要 startingNodes 了，因为在这个 synchronized 块里我们就会完成启动
        }

        // 3. 【关键修改】移除 Executor.submit，改为同步执行！
        // 这样如果 start() 抛出异常，Master 就能立刻收到 500 或者 success:false
        try {
            logger.info("Starting DataNode sync: nodeId={}, port={}", nodeId, port);

            // 路径检查与清理
            File targetDir = new File(dataDir); // 注意：这里通常直接用 dataDir 即可，或者确保路径逻辑一致
            if (!targetDir.exists()) {
                targetDir.mkdirs();
            }

            // 启动逻辑 (这一步会阻塞几百毫秒到1秒，直到Netty绑定端口成功)
            DataNode node = new DataNode(nodeId, dataDir, port, isPrimary, replicaNodes, replicationFactor);
            node.start(); // 假设这个方法如果端口占用会抛出异常

            // 4. 启动成功：放入 runningNodes
            runningNodes.put(nodeId, node);
            logger.info("DataNode {} started successfully", nodeId);

            // 5. 返回真正的成功信息
            return Map.of(
                    "success", true,
                    "message", "DataNode started successfully",
                    "nodeId", nodeId
            );

        } catch (Exception e) {
            logger.error("Failed to start DataNode {}: {}", nodeId, e.getMessage(), e);
            // 6. 如果失败，Master 必须知道！
            return Map.of(
                    "success", false,
                    "error", "Startup failed: " + e.getMessage(),
                    "nodeId", nodeId
            );
        }
    }
    /**
     * 停止节点
     */
    public Map<String, Object> stopNode(String nodeId) {
        DataNode node = runningNodes.get(nodeId);
        if (node == null) {
            return Map.of("success", false, "error", "Node " + nodeId + " not found");
        }

        logger.info("Stopping DataNode: {}", nodeId);

        executor.submit(() -> {
            try {
                node.stop();
                runningNodes.remove(nodeId);
                logger.info("DataNode {} stopped successfully", nodeId);
            } catch (Exception e) {
                logger.error("Error stopping DataNode {}: {}", nodeId, e.getMessage(), e);
            }
        });

        return Map.of("success", true, "message", "DataNode stop requested", "nodeId", nodeId);
    }

    /**
     * 获取状态
     */
    public Map<String, Object> getStatus(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            // 返回所有节点
            List<Map<String, Object>> list = new ArrayList<>();
            runningNodes.forEach((k, v) -> {
                Map<String, Object> info = new HashMap<>();
                info.put("nodeId", k);
                info.put("running", v.isHealthy()); // 假设有 isHealthy 方法
                list.add(info);
            });
            return Map.of("success", true, "count", list.size(), "nodes", list);
        } else {
            // 返回单个节点
            DataNode node = runningNodes.get(nodeId);
            if (node == null) {
                return Map.of("success", false, "error", "Node not found");
            }
            return Map.of("success", true, "nodeId", nodeId, "status", node.isHealthy() ? "RUNNING" : "STOPPED");
        }
    }

    /**
     * Agent 健康检查
     */
    public Map<String, Object> getHealth() {
        return Map.of(
                "status", "UP",
                "service", "DKV DataNode Agent",
                "activeNodes", runningNodes.size(),
                "timestamp", System.currentTimeMillis()
        );
    }

    /**
     * Spring 容器关闭时自动调用，清理资源
     */
    @PreDestroy
    public void cleanup() {
        logger.info("Agent is shutting down, stopping all DataNodes...");
        runningNodes.values().forEach(node -> {
            try {
                node.stop();
            } catch (Exception e) {
                logger.error("Error stopping node during shutdown", e);
            }
        });
        executor.shutdownNow();
    }
    /**
     * 获取所有正在运行的节点信息
     * @return 返回所有正在运行的 DataNode 实例的列表
     */
    public Map<String, DataNode> getRunningNodes() {
        return new HashMap<>(runningNodes);  // 返回一个副本，避免外部修改原始数据
    }

}