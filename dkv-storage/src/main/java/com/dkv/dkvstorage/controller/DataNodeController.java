package com.dkv.dkvstorage.controller;

import com.dkv.dkvstorage.rocksdb.DataNodeManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/datanode")
public class DataNodeController {

    @Autowired
    private DataNodeManager dataNodeManager;

    /**
     * 启动DataNode
     * curl -X POST "http://localhost:8080/api/datanode/start" \
     *   -H "Content-Type: application/json" \
     *   -d '{
     *     "nodeId": "node1:9000",
     *     "dataDir": "/tmp/datanode1",
     *     "port": 9000,
     *     "isPrimary": true,
     *     "replicas": "node2:9001,node3:9002"
     *   }'
     */
    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startDataNode(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();

        try {
            String nodeId = (String) request.get("nodeId");
            String host = (String) request.get("host");
            int port = ((Number) request.get("port")).intValue();
            String dataDir = (String) request.get("dataDir");
            boolean isPrimary = (boolean) request.get("isPrimary");
            String replicas = (String) request.get("replicas");

            boolean success = dataNodeManager.startDataNode(nodeId, host, port, dataDir, isPrimary, replicas);

            if (success) {
                response.put("success", true);
                response.put("message", "DataNode start requested");
                response.put("nodeId", nodeId);
                return ResponseEntity.accepted().body(response);  // 202 Accepted
            } else {
                response.put("success", false);
                response.put("message", "Failed to start DataNode");
                return ResponseEntity.badRequest().body(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 停止DataNode
     * curl -X DELETE "http://localhost:8080/api/datanode/stop" \
     *   -H "Content-Type: application/json" \
     *   -d '{"nodeId": "node1:9000"}'
     */
    @DeleteMapping("/stop")
    public ResponseEntity<Map<String, Object>> stopDataNode(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();

        try {
            String nodeId = (String) request.get("nodeId");
            boolean success = dataNodeManager.stopDataNode(nodeId);

            if (success) {
                response.put("success", true);
                response.put("message", "DataNode stop requested");
                response.put("nodeId", nodeId);
                return ResponseEntity.accepted().body(response);
            } else {
                response.put("success", false);
                response.put("message", "DataNode not found or already stopped");
                return ResponseEntity.badRequest().body(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 获取节点状态
     * curl "http://localhost:8080/api/datanode/status?nodeId=node1:9000"
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getNodeStatus(@RequestParam String nodeId) {
        Map<String, Object> response = new HashMap<>();

        try {
            Map<String, Object> status = dataNodeManager.getNodeStatus(nodeId);

            if ("NOT_FOUND".equals(status.get("status"))) {
                response.put("success", false);
                response.put("message", status.get("error"));
                return ResponseEntity.badRequest().body(response);
            }

            response.put("success", true);
            response.putAll(status);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Failed to get node status: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 获取所有节点
     * curl "http://localhost:8080/api/datanode/nodes"
     */
    @GetMapping("/nodes")
    public ResponseEntity<Map<String, Object>> getAllNodes() {
        Map<String, Object> response = new HashMap<>();

        try {
            var nodes = dataNodeManager.getAllNodes();

            response.put("success", true);
            response.put("count", nodes.size());
            response.put("nodes", nodes);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Failed to list nodes: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

//    /**
//     * 集群健康检查
//     * curl "http://localhost:8080/api/datanode/health"
//     */
//    @GetMapping("/health")
//    public ResponseEntity<Map<String, Object>> healthCheck() {
//        Map<String, Object> response = new HashMap<>();
//
//        try {
//            var nodes = dataNodeManager.getAllNodes();
//            long runningCount = nodes.stream()
//                    .filter(n -> "RUNNING".equals(n.get("status")))
//                    .count();
//
//            response.put("status",nodes.size() > runningCount / 2 ? "healthy" : "unhealthy");
//            response.put("timestamp", System.currentTimeMillis());
//            response.put("totalNodes", nodes.size());
//            response.put("runningNodes", runningCount);
//            response.put("success", true);
//
//            return ResponseEntity.ok(response);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            response.put("status", "unhealthy");
//            response.put("error", e.getMessage());
//            response.put("success", false);
//            return ResponseEntity.internalServerError().body(response);
//        }
//    }
}