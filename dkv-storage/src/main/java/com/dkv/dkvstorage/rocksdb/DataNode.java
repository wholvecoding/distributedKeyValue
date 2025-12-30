package com.dkv.dkvstorage.rocksdb;
// DataNode.java
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

@Data
public class DataNode {
    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private  String nodeId;
    private  String dataDir;
    private  int port;
    private  boolean isPrimary;
    private  List<String> replicaNodes;
    private  int replicationFactor;

    private StorageEngine storageEngine;
    private ReplicationService replicationService;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;


    public DataNode(String nodeId, String dataDir, int port,
                    boolean isPrimary, List<String> replicaNodes, int replicationFactor) {
        this.nodeId = nodeId;
        this.dataDir = dataDir;
        this.port = port;
        this.isPrimary = isPrimary;
        this.replicaNodes = replicaNodes;
        this.replicationFactor = replicationFactor;
    }

    // 如果其他地方也报错，可能还需要加 getNodeId() 等

    public List<String> getReplicas(){
        return this.replicaNodes;
    }
    public void start() throws Exception {
        logger.info("Starting DataNode {} on port {}", nodeId, port);
        logger.info("Data directory: {}", dataDir);
        logger.info("Is primary: {}", isPrimary);
        logger.info("Replica nodes: {}", replicaNodes);

        // 1. 初始化存储引擎
        storageEngine = new RocksDbEngine();
        storageEngine.init(dataDir);

        // 2. 初始化复制服务
        replicationService = new ReplicationService(storageEngine, replicaNodes, isPrimary, replicationFactor);

        // 3. 启动Netty服务器
        startNettyServer(false);

        registerToZookeeper("127.0.0.1:2181");

        logger.info("DataNode {} started successfully", nodeId);
    }

    private void startNettyServer(Boolean isPrior) throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(4);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // 添加对象编解码器
                            pipeline.addLast(new ObjectEncoder());
                            pipeline.addLast(new ObjectDecoder(
                                    ClassResolvers.cacheDisabled(null)));

                            // 添加业务处理器
                            pipeline.addLast(new DkvServerHandler(
                                    storageEngine, replicationService, isPrior));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口，开始接收连接
            ChannelFuture f = b.bind(port).sync();
            serverChannel = f.channel();

            logger.info("Netty server started on port {}", port);

        } catch (Exception e) {
            logger.error("Failed to start Netty server", e);
            throw e;
        }
    }

    public void stop() {
        logger.info("Stopping DataNode {}", nodeId);

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (replicationService != null) {
            replicationService.shutdown();
        }

        if (storageEngine != null) {
            storageEngine.close();
        }

        logger.info("DataNode {} stopped", nodeId);
    }

    // 健康检查
    public boolean isHealthy() {
        return serverChannel != null && serverChannel.isActive();
    }


    private void registerToZookeeper(String zkAddress ) throws Exception {
        // 这里使用 Curator 框架简单实现，或者调用你已经写好的工具类
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();
        String host = this.nodeId.split(":")[0];
        String path = "/dkv/nodes/" +host + ":"+this.port;
        if (client.checkExists().forPath(path) == null) {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path);
            logger.info("注册成功: {}", path);
        }
    }
    public void updateReplicationService(List<String> newReplicaNodes, int newReplicationFactor) throws Exception {
        logger.info("收到更新指令，准备热重启...");

        //Step A: 【必须】先关掉旧的监听，释放 7007 端口
        shutdownNettyOnly();

        // Step B: 更新内存配置
        this.replicaNodes = new ArrayList<>(newReplicaNodes);
        this.replicationFactor = newReplicationFactor;
        logger.info("当前的所有从节点，"+this.replicaNodes);
        // Step C: 创建“第二版”服务（包含分发逻辑）
        this.replicationService = new ReplicationService(storageEngine, replicaNodes, true, replicationFactor);

        // Step D: 【必须】重新启动监听！
        // 这次启动后，Netty 用的是上面 Step C 创建的新 service
        // 所以它拥有了分发功能！
        startNettyServer(true);

        logger.info("热重启完成，现在支持分发到: {}", newReplicaNodes);
    }
    private void shutdownNettyOnly() {
        // 优雅关闭，确保端口释放
        if (serverChannel != null) {
            try { serverChannel.close().sync(); } catch (Exception e) {}
            serverChannel = null;
        }
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        // 置为 null 很重要，方便下一次 startNettyServer 重新 new
        bossGroup = null;
        workerGroup = null;
    }

}