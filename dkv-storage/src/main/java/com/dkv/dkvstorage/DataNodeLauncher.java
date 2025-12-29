package com.dkv.dkvstorage;

import com.dkv.dkvstorage.rocksdb.DataNode;
import java.util.Arrays;
import java.util.List;

public class DataNodeLauncher {

    public static void main(String[] args) throws Exception {

        String nodeId = System.getProperty("nodeId");
        System.out.println(nodeId);
        String dataDir = System.getProperty("dataDir");
        System.out.println(dataDir);
        int port = Integer.parseInt(System.getProperty("port"));
        System.out.println(port);
        boolean isPrimary = Boolean.parseBoolean(System.getProperty("isPrimary"));
        System.out.println(isPrimary);
        String replicas = System.getProperty("replicas", "");
        System.out.println(replicas);
        List<String> replicaNodes = replicas.isEmpty()
                ? List.of()
                : Arrays.asList(replicas.split(","));

        int replicationFactor = replicaNodes.size() + 1;

        DataNode node = new DataNode(
                nodeId,
                dataDir,
                port,
                isPrimary,
                replicaNodes,
                replicationFactor
        );

        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
    }
}

