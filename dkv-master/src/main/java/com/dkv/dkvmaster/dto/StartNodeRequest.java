package com.dkv.dkvmaster.dto;

public class StartNodeRequest {
    private String nodeId;
    private String host;
    private Integer port;
    private String dataDir;
    private Boolean isPrimary = true;
    private String replicas = "";

    // 可选：是否同步加入一致性哈希环
    private Boolean addToRing = true;

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public Boolean getIsPrimary() { return isPrimary; }
    public void setIsPrimary(Boolean primary) { isPrimary = primary; }

    public String getReplicas() { return replicas; }
    public void setReplicas(String replicas) { this.replicas = replicas; }

    public Boolean getAddToRing() { return addToRing; }
    public void setAddToRing(Boolean addToRing) { this.addToRing = addToRing; }
}
