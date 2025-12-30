package com.dkv.dkvstorage.controller;


import com.dkv.dkvstorage.rocksdb.DataNode;
import com.dkv.dkvstorage.service.DataNodeAgentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/agent") // 基础路径
@CrossOrigin(origins = "*")
public class AgentController {

    @Autowired
    private DataNodeAgentService agentService;


    @PostMapping("/command")
    public ResponseEntity<Map<String, Object>> handleCommand(@RequestBody Map<String, Object> payload) {
        String action = (String) payload.get("action");
        if (action == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Missing action"));
        }

        Map<String, Object> result;
        try {
            switch (action) {
                case "start":
                    result = agentService.startNode(
                            (String) payload.get("nodeId"),
                            (String) payload.get("dataDir"),
                            getInt(payload, "port"),
                            (boolean) payload.getOrDefault("isPrimary", false),
                            (String) payload.getOrDefault("replicas", "")
                    );
                    break;
                case "stop":
                    result = agentService.stopNode((String) payload.get("nodeId"));
                    break;
                case "status":
                    result = agentService.getStatus((String) payload.get("nodeId"));
                    break;
                case "health":
                    result = agentService.getHealth();
                    break;
                default:
                    return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Unknown action"));
            }
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("success", false, "error", e.getMessage()));
        }

        return ResponseEntity.ok(result);
    }

    // 辅助方法：安全获取 Integer
    private int getInt(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        throw new IllegalArgumentException("Invalid integer for key: " + key);
    }

    // 你也可以保留一个简单的 GET 用于浏览器快速检查
    @GetMapping("/health")
    public Map<String, Object> healthCheck() {
        return agentService.getHealth();
    }

    // ==========================================
    // 1. 提取公共方法：将 DataNode 转为 Map
    // ==========================================
    private Map<String, Object> convertNodeToMap(DataNode node) {
        Map<String, Object> info = new HashMap<>();
        info.put("nodeId", node.getNodeId());
        info.put("port", node.getPort());
        info.put("isPrimary", node.isPrimary());
        info.put("healthy", node.isHealthy());

        // 修正点：根据之前的 DataNode 定义，这里应该是 getReplicaNodes()
        // 如果你的 DataNode 里确实叫 getReplicas() 就保持原样
        info.put("replicas", node.getReplicaNodes());

        // 提取 Host
        String host = node.getNodeId().split(":")[0];
        info.put("host", host);
        return info;
    }

    // ==========================================
    // 2. 原有的获取列表（优化后）
    // ==========================================
    @GetMapping("/nodes")
    public List<Map<String, Object>> getAllRunningNodes() {
        return agentService.getRunningNodes().values().stream()
                .map(this::convertNodeToMap) // 复用上面的逻辑
                .collect(Collectors.toList());
    }

    // ==========================================
    // 3. 【新增】根据 NodeId (IP:Port) 获取单个节点
    // API: GET /api/node?nodeId=127.0.0.1:7007
    // ==========================================
    @GetMapping("/node")
    public ResponseEntity<Map<String, Object>> getNodeDetail(@RequestParam String nodeId) {
        DataNode node = agentService.getRunningNodes().get(nodeId);

        if (node == null) {
            return ResponseEntity.status(404)
                    .body(Map.of("error", "Node not found: " + nodeId));
        }

        return ResponseEntity.ok(convertNodeToMap(node));
    }

    // 建议使用 PostMapping，因为这是在修改服务器状态
    @GetMapping("/set")
    public ResponseEntity<Map<String, Object>> setNodeWithReplicas(
            @RequestParam String nodeId,
            @RequestParam List<String> replicas) throws Exception { // Spring 自动将逗号分隔字符串转为 List

        DataNode node = agentService.getRunningNodes().get(nodeId);

        if (node == null) {
            return ResponseEntity.status(404)
                    .body(Map.of("error", "Node not found: " + nodeId));
        }

        // 1. 更新副本列表
        node.setReplicaNodes(replicas);

        // 2. 同时更新复制因子 (通常是 副本数 + 1个主节点)
        node.setReplicationFactor(replicas.size() );

        // 3. 【重要】如果 ReplicationService 正在运行，可能需要通知它更新配置
        // 如果你的 ReplicationService 只是启动时读取一次配置，那么这里修改可能不会立即生效
        // 需要查看 ReplicationService 是否有 updateConfig 之类的方法
        if (node.getReplicationService() != null) {
            // node.getReplicationService().updateReplicas(replicas); // 如果有这个方法的话
        }
        node.updateReplicationService(replicas,replicas.size() );
        return ResponseEntity.ok(convertNodeToMap(node));
    }
    // ==========================================
    // 4. 【新增】根据纯 IP 获取该 IP 下的所有节点
    // API: GET /api/nodes/by-ip?ip=127.0.0.1
    // ==========================================
    @GetMapping("/nodes/by-ip")
    public List<Map<String, Object>> getNodesByIp(@RequestParam String ip) {
        return agentService.getRunningNodes().values().stream()
                // 过滤条件：NodeId 是否以该 IP 开头
                .filter(node -> node.getNodeId().startsWith(ip + ":"))
                .map(this::convertNodeToMap)
                .collect(Collectors.toList());
    }
}
