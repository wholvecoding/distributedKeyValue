package com.dkv.dkvstorage.rocksdb;

import com.dkv.dkvstorage.agent.NettyAgentClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;

@Component
public class DataNodeManager {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeManager.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${datanode.agent.port:8085}")
    private int agentPort = 8085;
    private static final String AGENT_URL = "http://127.0.0.1:8085/agent/command";
    @Value("${datanode.netty.connect.timeout:5000}")
    private int connectTimeout = 5000;

    // 节点信息映射
    private final Map<String, NodeInfo> nodeRegistry = new ConcurrentHashMap<>();
    // Netty客户端连接池
    private final Map<String, NettyAgentClient> clientPool = new ConcurrentHashMap<>();
    // 定时任务执行器
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final RestTemplate restTemplate = new RestTemplate();
    public DataNodeManager() {
        System.out.println("DataNodeManager 实例被创建: " + System.identityHashCode(this));
    }
    public static class NodeInfo {
        private String nodeId;
        private String host;
        private int port;
        private String dataDir;
        private boolean isPrimary;
        private List<String> replicas;
        private long startTime;
        private NodeStatus status = NodeStatus.REGISTERED;

        public NodeInfo(String nodeId, String host, int port, String dataDir,
                        boolean isPrimary, List<String> replicas) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.dataDir = dataDir;
            this.isPrimary = isPrimary;
            this.replicas = replicas;
            this.startTime = System.currentTimeMillis();
        }

        // getters and setters
        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getDataDir() { return dataDir; }
        public boolean isPrimary() { return isPrimary; }
        public List<String> getReplicas() { return replicas; }
        public long getStartTime() { return startTime; }
        public NodeStatus getStatus() { return status; }
        public void setStatus(NodeStatus status) { this.status = status; }
    }

    public enum NodeStatus {
        REGISTERED,      // 已注册
        STARTING,        // 启动中
        RUNNING,         // 运行中
        STOPPING,        // 停止中
        STOPPED,         // 已停止
        ERROR           // 错误状态
    }

    /**
     * 启动DataNode（分布式版本）
     */
    public boolean startDataNode(String nodeId, String host, int port, String dataDir, boolean isPrimary, String replicas) {
        // 1. 本地注册信息
        List<String> replicaList = Arrays.asList((replicas != null ? replicas : "").split(","));
        NodeInfo nodeInfo = new NodeInfo(nodeId, host, port, dataDir, isPrimary, replicaList);
        nodeInfo.setStatus(NodeStatus.STARTING);
        nodeRegistry.put(nodeId, nodeInfo);

        // 2. 发送 HTTP 请求给 Agent
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("action", "start");
            payload.put("nodeId", nodeId);
            payload.put("host", host);
            payload.put("port", port);
            payload.put("dataDir", dataDir);
            payload.put("isPrimary", isPrimary);
            payload.put("replicas", replicas);

            // 发送 POST 请求
            ResponseEntity<Map> response = restTemplate.postForEntity(
                    AGENT_URL,
                    payload,
                    Map.class
            );

            if (response.getStatusCode() == HttpStatus.OK &&
                    Boolean.TRUE.equals(response.getBody().get("success"))) {

                logger.info("Agent successfully started node {}", nodeId);
                nodeInfo.setStatus(NodeStatus.RUNNING);
                return true;
            } else {
                logger.error("Agent failed to start node: {}", response.getBody());
                nodeInfo.setStatus(NodeStatus.ERROR);
                return false;
            }
        } catch (Exception e) {
            logger.error("Failed to communicate with Agent at {}", AGENT_URL, e);
            nodeInfo.setStatus(NodeStatus.ERROR);
            return false;
        }
    }

    /**
     * 停止 DataNode
     */
    public boolean stopDataNode(String nodeId) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("action", "stop");
            payload.put("nodeId", nodeId);

            restTemplate.postForEntity(AGENT_URL, payload, Map.class);

            nodeRegistry.remove(nodeId);
            return true;
        } catch (Exception e) {
            logger.error("Failed to stop node via Agent", e);
            return false;
        }
    }
    /**
     * 内部启动逻辑
     */
    private boolean startNodeInternal(NodeInfo nodeInfo) throws Exception {
        String nodeId = nodeInfo.getNodeId();
        String host = nodeInfo.getHost();

        // 获取或创建Netty客户端
        NettyAgentClient client = getOrCreateClient(host);

        // 构建启动请求
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", nodeId);
        request.put("dataDir", nodeInfo.getDataDir());
        request.put("port", nodeInfo.getPort());
        request.put("isPrimary", nodeInfo.isPrimary());
        request.put("replicas", String.join(",", nodeInfo.getReplicas()));
        request.put("action", "start");

        // 发送请求到Agent
        Map<String, Object> response = client.sendRequest(request);

        if (response != null && Boolean.TRUE.equals(response.get("success"))) {
            logger.info("Start command sent successfully to {}", host);
            return true;
        } else {
            String errorMsg = response != null ? (String) response.get("error") : "No response";
            logger.error("Failed to start node on {}: {}", host, errorMsg);
            return false;
        }
    }

    /**
     * 内部停止逻辑
     */
    private boolean stopNodeInternal(NodeInfo nodeInfo) throws Exception {
        String nodeId = nodeInfo.getNodeId();
        String host = nodeInfo.getHost();

        NettyAgentClient client = getOrCreateClient(host);

        // 构建停止请求
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", nodeId);
        request.put("action", "stop");

        Map<String, Object> response = client.sendRequest(request);

        if (response != null && Boolean.TRUE.equals(response.get("success"))) {
            logger.info("Stop command sent successfully to {}", host);
            return true;
        } else {
            String errorMsg = response != null ? (String) response.get("error") : "No response";
            logger.error("Failed to stop node on {}: {}", host, errorMsg);
            return false;
        }
    }

    /**
     * 获取或创建Netty客户端
     */
    private synchronized NettyAgentClient getOrCreateClient(String host) throws Exception {
        String clientKey = host + ":" + agentPort;

        if (!clientPool.containsKey(clientKey)) {
            NettyAgentClient client = new NettyAgentClient(host, agentPort);
            client.connect();
            clientPool.put(clientKey, client);
            logger.info("Created Netty client for {}:{}", host, agentPort);
        }

        return clientPool.get(clientKey);
    }

    /**
     * 获取节点状态
     */
    public Map<String, Object> getNodeStatus(String nodeId) {
        NodeInfo nodeInfo = nodeRegistry.get(nodeId);
        if (nodeInfo == null) {
            return Map.of(
                    "nodeId", nodeId,
                    "status", "NOT_FOUND",
                    "error", "Node not registered"
            );
        }

        try {
            // 尝试从Agent获取实时状态
            NettyAgentClient client = getOrCreateClient(nodeInfo.getHost());
            Map<String, Object> request = new HashMap<>();
            request.put("nodeId", nodeId);
            request.put("action", "status");

            Map<String, Object> agentResponse = client.sendRequest(request);

            Map<String, Object> result = new HashMap<>();
            result.put("nodeId", nodeId);
            result.put("registryStatus", nodeInfo.getStatus().name());

            if (agentResponse != null && agentResponse.containsKey("status")) {
                result.put("agentStatus", agentResponse.get("status"));
                result.put("uptime", System.currentTimeMillis() - nodeInfo.getStartTime());
            }

            return result;

        } catch (Exception e) {
            return Map.of(
                    "nodeId", nodeId,
                    "status", nodeInfo.getStatus().name(),
                    "error", "Failed to query agent: " + e.getMessage()
            );
        }
    }

    /**
     * 获取所有节点信息
     */
    public List<Map<String, Object>> getAllNodes() {
        List<Map<String, Object>> result = new ArrayList<>();

        for (NodeInfo nodeInfo : nodeRegistry.values()) {
            Map<String, Object> nodeData = new HashMap<>();
            nodeData.put("nodeId", nodeInfo.getNodeId());
            nodeData.put("host", nodeInfo.getHost());
            nodeData.put("port", nodeInfo.getPort());
            nodeData.put("isPrimary", nodeInfo.isPrimary());
            nodeData.put("status", nodeInfo.getStatus().name());
            nodeData.put("startTime", nodeInfo.getStartTime());
            nodeData.put("replicas", nodeInfo.getReplicas());

            result.add(nodeData);
        }

        return result;
    }

    /**
     * 调度健康检查
     */
    private void scheduleHealthCheck(String nodeId) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                Map<String, Object> status = getNodeStatus(nodeId);
                NodeInfo nodeInfo = nodeRegistry.get(nodeId);

                if (nodeInfo != null) {
                    // 更新状态
                    Object running = status.get("status");
                    if (running != null && "RUNNING".equals(running)) {
                        nodeInfo.setStatus(NodeStatus.RUNNING);
                    } else if (running != null && "STOPPED".equals(running)) {
                        nodeInfo.setStatus(NodeStatus.STOPPED);
                    }

                    logger.debug("Health check for {}: {}", nodeId, status);
                }
            } catch (Exception e) {
                logger.warn("Health check failed for {}: {}", nodeId, e.getMessage());
            }
        }, 10, 120, TimeUnit.SECONDS);  // 10秒后开始，每30秒检查一次
    }

//    /**
//     * 工具方法：从nodeId提取主机
//     */
//    private String extractHostFromNodeId(String nodeId) {
//        if (nodeId.contains(":")) {
//            return nodeId.split(":")[0];
//        } else if (nodeId.equals("localhost") || nodeId.equals("127.0.0.1")) {
//            return nodeId;
//        } else {
//            // 假设nodeId就是主机名
//            return nodeId;
//        }
//    }

    /**
     * 工具方法：解析副本列表
     */
    private List<String> parseReplicaList(String replicas) {
        if (replicas == null || replicas.trim().isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.asList(replicas.split(","));
    }

    /**
     * 关闭管理器
     */
    public void shutdown() {
        logger.info("Shutting down DataNodeManager...");

        // 停止所有节点
        for (String nodeId : new ArrayList<>(nodeRegistry.keySet())) {
            try {
                stopDataNode(nodeId);
            } catch (Exception e) {
                logger.warn("Error stopping node {} during shutdown: {}", nodeId, e.getMessage());
            }
        }

        // 关闭客户端连接
        for (Map.Entry<String, NettyAgentClient> entry : clientPool.entrySet()) {
            try {
                entry.getValue().disconnect();
            } catch (Exception e) {
                logger.warn("Error disconnecting client {}: {}", entry.getKey(), e.getMessage());
            }
        }
        clientPool.clear();

        // 关闭调度器
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("DataNodeManager shutdown complete");
    }
}