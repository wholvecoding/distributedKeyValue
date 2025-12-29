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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class DataNode {
    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private final String nodeId;
    private final String dataDir;
    private final int port;
    private final boolean isPrimary;
    private final List<String> replicaNodes;
    private final int replicationFactor;

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
        startNettyServer();

        startNettyServer();
//        registerToZookeeper("127.0.0.1:2181");

        logger.info("DataNode {} started successfully", nodeId);
    }

    private void startNettyServer() throws Exception {
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
                                    storageEngine, replicationService, isPrimary));
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

        String path = "/dkv/nodes/" + this.nodeId + ":"+this.port;
        if (client.checkExists().forPath(path) == null) {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path);
            logger.info("注册成功: {}", path);
        }
    }

}