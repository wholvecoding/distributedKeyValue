package com.dkv.dkvstorage.controller;


import com.dkv.dkvstorage.service.DataNodeAgentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/agent") // 基础路径
@CrossOrigin(origins = "*")
public class AgentController {

    @Autowired
    private DataNodeAgentService agentService;

    /**
     * 统一入口：兼容之前的 {action: "start", ...} 格式
     * 如果想做更标准的 REST，可以拆分为 @PostMapping("/start") 等
     */
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
}
