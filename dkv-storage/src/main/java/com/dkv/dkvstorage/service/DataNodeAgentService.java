package com.dkv.dkvstorage.service;

import com.dkv.dkvstorage.rocksdb.DataNode; // 假设这是你的 RocksDB 包装类
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy; // Spring Boot 3.x 使用 jakarta, 2.x 使用 javax
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
    public Map<String, Object> startNode(String nodeId, String dataDir, int port, boolean isPrimary, String replicasStr) {
        if (runningNodes.containsKey(nodeId)) {
            return Map.of("success", false, "error", "Node " + nodeId + " is already running");
        }

        // 解析副本配置
        List<String> replicaNodes = new ArrayList<>();
        if (replicasStr != null && !replicasStr.isEmpty()) {
            replicaNodes.addAll(Arrays.asList(replicasStr.split(",")));
        }
        int replicationFactor = replicaNodes.size() + 1;

        logger.info("Starting DataNode: nodeId={}, port={}", nodeId, port);

        // 异步执行启动逻辑（因为 RocksDB 初始化可能耗时）
        executor.submit(() -> {
            try {
                // 这里调用你原本的 DataNode 构造和启动逻辑
                DataNode node = new DataNode(nodeId, dataDir, port, isPrimary, replicaNodes, replicationFactor);
                node.start();

                runningNodes.put(nodeId, node);
                logger.info("DataNode {} started successfully on port {}", nodeId, port);
            } catch (Exception e) {
                logger.error("Failed to start DataNode {}: {}", nodeId, e.getMessage(), e);
            }
        });

        // 立即返回 "请求已接收"
        return Map.of(
                "success", true,
                "message", "DataNode start requested (async)",
                "nodeId", nodeId
        );
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
}