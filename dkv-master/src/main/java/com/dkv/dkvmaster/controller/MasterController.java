package com.dkv.dkvmaster.controller;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.dto.StartNodeRequest;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import com.dkv.dkvmaster.service.NodeOrchestrationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin // 允许跨域访问
public class MasterController {

    private final ConsistentHashRouter router;
    private final ClusterManager clusterManager;
    private final NodeOrchestrationService orchestrationService;

    public MasterController(ConsistentHashRouter router, ClusterManager clusterManager, NodeOrchestrationService orchestrationService) {
        this.router = router;
        this.clusterManager = clusterManager;
        this.orchestrationService = orchestrationService;
    }

    // 查看当前所有在线节点
    @GetMapping("/home")
    public String home() {
        return "here is home";
    }
    @GetMapping("/nodes")
    public List<String> getOnlineNodes() {
        return clusterManager.getOnlineNodes();
    }
    @GetMapping("/add")
    public void addNodes(@RequestParam("nodeip") String nodeIp,@RequestParam("port")Integer port){
         router.addNode(nodeIp,port);
    }
    @PostMapping("/addtozk")
    public void addNodes2ZK(@RequestParam("nodeip") String nodeIp, @RequestParam("port")Integer port  ) throws Exception {
        clusterManager.addNodeToZk(nodeIp,port);

    }
    @PostMapping("/delete")
    public void deleteNode(@RequestParam("nodeip") String nodeIp,@RequestParam("port") Integer port) throws Exception {

        clusterManager.offlineNode(nodeIp, port);
    }
    @GetMapping("/debug/ring")
    public Object getRing() {
        // 打印当前环中所有去重后的物理节点
        return router.getRings();
    }
    // 查询 Key 的路由信息
    @GetMapping("/route")
        public Map<String, Object> getRoute(@RequestParam String key, @RequestParam(defaultValue = "3") int replicas) {
        List<String> targets = router.routeNodeWithReplicas(key, replicas);
        Map<String, Object> response = new HashMap<>();
        response.put("key", key);
        response.put("hash", String.format("0x%08X", key.hashCode())); // 展示哈希值
        response.put("primary", targets.isEmpty() ? "None" : targets.get(0));
        response.put("secondary", targets.size() > 1 ? targets.subList(1, targets.size()) : Collections.emptyList());
        response.put("replicas", targets.size() <= 1 ? Collections.emptyList() : targets.subList(1, targets.size()));
        return response;
    }

    @PostMapping("/datanode/start-and-join-zk")
    public ResponseEntity<Map<String, Object>> startAndJoinZk(@RequestBody StartNodeRequest request) {
        try {
            Map<String, Object> res = orchestrationService.startAndRegister(request);
            boolean ok = Boolean.TRUE.equals(res.get("success"));
            return ok ? ResponseEntity.ok(res) : ResponseEntity.badRequest().body(res);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "stage", "UNEXPECTED", "error", e.getMessage()));
        }
    }
}