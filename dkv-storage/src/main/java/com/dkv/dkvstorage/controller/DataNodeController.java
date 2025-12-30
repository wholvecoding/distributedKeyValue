package com.dkv.dkvstorage.controller;

import com.dkv.dkvstorage.rocksdb.DataNodeManager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.channel.*;
/**
 * DataNode 实例生命周期管理
 */
@RestController
@RequestMapping("/api/datanode")
public class DataNodeController {


    @Autowired
    private DataNodeManager dataNodeManager;
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startDataNode(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String nodeId = (String) request.get("nodeId");
            String host = (String) request.get("host");
            int port = ((Number) request.get("port")).intValue();
            String dataDir = (String) request.get("dataDir");
            boolean isPrimary = (boolean) request.getOrDefault("isPrimary", true);
            String replicas = (String) request.getOrDefault("replicas", "");

            boolean success = dataNodeManager.startDataNode(nodeId, host, port, dataDir, isPrimary, replicas);

            if (success) {
                response.put("success", true);
                response.put("nodeId", nodeId);
                return ResponseEntity.accepted().body(response);
            } else {
                response.put("success", false);
                response.put("message", "DataNode 启动失败，请检查端口是否冲突或路径是否合法");
                return ResponseEntity.badRequest().body(response);
            }
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @DeleteMapping("/stop")
    public ResponseEntity<Map<String, Object>> stopDataNode(@RequestBody Map<String, Object> request) {
        String nodeId = (String) request.get("nodeId");
        if (dataNodeManager.stopDataNode(nodeId)) {
            return ResponseEntity.ok(Map.of("success", true, "message", "节点已停止", "nodeId", nodeId));
        }
        return ResponseEntity.badRequest().body(Map.of("success", false, "message", "节点未找到或已停止"));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getNodeStatus(@RequestParam String nodeId) {
        Map<String, Object> status = dataNodeManager.getNodeStatus(nodeId);
        if (status.containsKey("error")) {
            return ResponseEntity.status(404).body(status);
        }
        return ResponseEntity.ok(status);
    }
    @GetMapping("/nodes")
    public ResponseEntity<Map<String, Object>> getAllNodes() {
        try {
            var nodes = dataNodeManager.getAllNodes(); // 假设 DataNodeManager 依然有这个方法

            // 使用新版风格的简写 Map.of 不太适合这里，因为 nodes 是列表
            // 保持原有的结构
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("count", nodes.size());
            response.put("nodes", nodes);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "message", "Failed to list nodes: " + e.getMessage()));
        }
    }

}