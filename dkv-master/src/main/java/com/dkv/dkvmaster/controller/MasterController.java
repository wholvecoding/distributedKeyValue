package com.dkv.dkvmaster.controller;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.dto.StartNodeRequest;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import com.dkv.dkvmaster.service.NodeOrchestrationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin // 允许跨域访问
@Tag(name = "Master节点管理接口", description = "提供节点上下线、路由查询及集群编排功能") // 替代 @Api
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
    @GetMapping("/nodes")
    @Operation(summary = "查看在线节点", description = "返回当前集群中所有在线节点的列表") // 替代 @ApiOperation
    public List<String> getOnlineNodes() {
        return clusterManager.getOnlineNodes();
    }

    @GetMapping("/add")
    @Operation(summary = "手动添加节点到路由(仅内存)", description = "仅更新本地路由表，不涉及ZK交互")
    public void addNodes(
            @Parameter(description = "节点IP地址", required = true, example = "192.168.1.10") @RequestParam("nodeip") String nodeIp, // 替代 @ApiParam
            @Parameter(description = "节点端口", required = true, example = "8081") @RequestParam("port") Integer port) {
        router.addNode(nodeIp, port);
    }

    @PostMapping("/addtozk")
    @Operation(summary = "添加节点到Zookeeper", description = "将新节点注册到Zookeeper集群管理中")
    public void addNodes2ZK(
            @Parameter(description = "节点IP地址", required = true) @RequestParam("nodeip") String nodeIp,
            @Parameter(description = "节点端口", required = true) @RequestParam("port") Integer port) throws Exception {
        clusterManager.addNodeToZk(nodeIp, port);
    }

    @PostMapping("/delete")
    @Operation(summary = "节点下线", description = "将指定节点从集群中移除")
    public void deleteNode(
            @Parameter(description = "节点IP地址", required = true) @RequestParam("nodeip") String nodeIp,
            @Parameter(description = "节点端口", required = true) @RequestParam("port") Integer port) throws Exception {
        String nodeId = nodeIp+":"+port;
        orchestrationService.stop(nodeId);
        clusterManager.offlineNode(nodeIp, port);
    }

    @GetMapping("/debug/ring")
    @Operation(summary = "Debug: 查看哈希环", description = "打印当前一致性哈希环中所有去重后的物理节点信息")
    public Object getRing() {
        return router.getRings();
    }

    // 查询 Key 的路由信息
    @GetMapping("/route")
    @Operation(summary = "查询Key路由信息", description = "计算指定Key在一致性哈希环上的主节点及副本节点")
    public Map<String, Object> getRoute(
            @Parameter(description = "存储的Key", required = true) @RequestParam String key,
            @Parameter(description = "需要的副本数量") @RequestParam(defaultValue = "3") int replicas) {
        List<String> targets = router.routeNodeWithReplicas(key, replicas);
        Map<String, Object> response = new HashMap<>();
        response.put("key", key);
        response.put("hash", String.format("0x%08X", key.hashCode()));
        response.put("primary", targets.isEmpty() ? "None" : targets.get(0));
        response.put("secondary", targets.size() > 1 ? targets.subList(1, targets.size()) : Collections.emptyList());
        response.put("replicas", targets.size() <= 1 ? Collections.emptyList() : targets.subList(1, targets.size()));
        return response;
    }

    @PostMapping("/datanode/start-and-join-zk")
    @Operation(summary = "启动DataNode并加入ZK", description = "本地启动DataNode进程并自动注册")
    public ResponseEntity<Map<String, Object>> startAndJoinZk(
            @Parameter(description = "启动请求参数实体", required = true) @RequestBody StartNodeRequest request) {
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