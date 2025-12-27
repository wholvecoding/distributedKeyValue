package com.dkv.dkvmaster.controller;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin // 允许跨域访问
public class MasterController {

    private final ConsistentHashRouter router;
    private final ClusterManager clusterManager;

    public MasterController(ConsistentHashRouter router, ClusterManager clusterManager) {
        this.router = router;
        this.clusterManager = clusterManager;
    }

    // 查看当前所有在线节点
    @GetMapping("/nodes")
    public List<String> getOnlineNodes() {
        return clusterManager.getOnlineNodes();
    }
    @GetMapping("/add")
    public void addNodes(@RequestParam("nodeip") String nodeIp){
         router.addNode(nodeIp);
    }
    @GetMapping("/addtozk")
    public void addNodes2ZK(@RequestParam("nodeip") String nodeIp) throws Exception {
        clusterManager.addNodeToZk(nodeIp);
    }
    @PostMapping("/delete")
    public void deleteNode(@RequestParam("nodeip") String nodeIp){
        router.removeNode(nodeIp);
    }
    // 查询 Key 的路由信息
    @GetMapping("/route")
    public Map<String, Object> getRoute(@RequestParam String key, @RequestParam(defaultValue = "3") int replicas) {
        List<String> targets = router.routeNodeWithReplicas(key, replicas);
        Map<String, Object> response = new HashMap<>();
        response.put("key", key);
        response.put("hash", String.format("0x%08X", key.hashCode())); // 展示哈希值
        response.put("primary", targets.isEmpty() ? "None" : targets.get(0));
        response.put("replicas", targets.size() <= 1 ? Collections.emptyList() : targets.subList(1, targets.size()));
        return response;
    }
}