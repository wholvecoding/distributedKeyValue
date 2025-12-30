package com.dkv.dkvmaster.service;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.dto.StartNodeRequest;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import com.dkv.dkvstorage.rocksdb.DataNodeManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;


@Service
public class NodeOrchestrationService {

    @Autowired
    private final DataNodeManager dataNodeManager;
    private final ClusterManager clusterManager;
    private final ConsistentHashRouter router;

    public NodeOrchestrationService(
            DataNodeManager dataNodeManager,
            ClusterManager clusterManager,
            ConsistentHashRouter router
    ) {
        this.dataNodeManager = dataNodeManager;
        this.clusterManager = clusterManager;
        this.router = router;
    }

    public Map<String, Object> startAndRegister(StartNodeRequest req) throws Exception {
        String nodeId = req.getNodeId();
        String host = req.getHost();
        int port = req.getPort();
        String dataDir = req.getDataDir();
        boolean isPrimary = req.getIsPrimary() != null && req.getIsPrimary();
        String replicas = req.getReplicas() == null ? "" : req.getReplicas();

        Map<String, Object> result = new HashMap<>();
        result.put("nodeId", nodeId);
        result.put("host", host);
        result.put("port", port);

        // 1) start datanode
        boolean started = dataNodeManager.startDataNode(nodeId, host, port, dataDir, isPrimary, replicas);
        if (!started) {
            result.put("success", false);
            result.put("stage", "START_DATANODE");
            result.put("message", "DataNode 启动失败：端口冲突/路径非法/实例已存在等");
            return result;
        }

        // 2) register to zk (失败需要回滚)
        try {
            clusterManager.addNodeToZk(host, port);
            result.put("zkRegistered", true);
        } catch (Exception zkEx) {
            // 回滚：把刚启动的节点停掉，避免“启动了但集群不可见”
            dataNodeManager.stopDataNode(nodeId);

            result.put("success", false);
            result.put("stage", "REGISTER_ZK");
            result.put("zkRegistered", false);
            result.put("message", "注册 ZK 失败，已回滚 stop DataNode: " + zkEx.getMessage());
            return result;
        }

        // 3) optionally add to ring
        if (req.getAddToRing() != null && req.getAddToRing()) {
            router.addNode(host, port);
            result.put("ringAdded", true);
        } else {
            result.put("ringAdded", false);
        }

        result.put("success", true);
        result.put("stage", "DONE");
        return result;
    }
}
