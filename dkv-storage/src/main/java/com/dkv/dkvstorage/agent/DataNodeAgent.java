package com.dkv.dkvstorage.agent;
import com.dkv.dkvstorage.rocksdb.DataNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 简化的DataNode Agent，使用文本协议与Master通信
 */
public class DataNodeAgent {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeAgent.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int AGENT_PORT = 8085;

    private static final Map<String, DataNode> runningNodes = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) throws Exception {
        logger.info("Starting DataNode Agent on port {}", AGENT_PORT);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(4);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    // 替换掉 StringDecoder，加入 HTTP 支持
                                    .addLast(new io.netty.handler.codec.http.HttpServerCodec())
                                    .addLast(new io.netty.handler.codec.http.HttpObjectAggregator(65536))
                                    .addLast(new SimpleChannelInboundHandler<io.netty.handler.codec.http.FullHttpRequest>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, io.netty.handler.codec.http.FullHttpRequest msg) throws Exception {
                                            // 从 HTTP 中提取 Body 字符串
                                            String content = msg.content().toString(io.netty.util.CharsetUtil.UTF_8);
                                            // 逻辑交给原本的 Handler 或直接在这里处理
                                            logger.info("Received HTTP JSON: {}", content);

                                            // 这里可以调用你原来的解析逻辑...
                                            // 注意：HTTP 需要返回标准的 HTTP Response，不能只发 JSON 字符串
                                        }
                                    }
                                    )
                                    .addLast(new AgentCommandHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(AGENT_PORT).sync();
            logger.info("DataNode Agent started on port {}", AGENT_PORT);

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                shutdown();
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }));

            f.channel().closeFuture().sync();

        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class AgentCommandHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String jsonRequest) throws Exception {
            logger.debug("Received request: {}", jsonRequest);

            Map<String, Object> request = objectMapper.readValue(jsonRequest, Map.class);
            String action = (String) request.get("action");
            Map<String, Object> response = new HashMap<>();

            try {
                switch (action) {
                    case "start":
                        response = handleStart(request);
                        break;
                    case "stop":
                        response = handleStop(request);
                        break;
                    case "status":
                        response = handleStatus(request);
                        break;
                    case "health":
                        response = handleHealth();
                        break;
                    default:
                        response.put("error", "Unknown action: " + action);
                        response.put("success", false);
                }
            } catch (Exception e) {
                logger.error("Error handling action {}: {}", action, e.getMessage(), e);
                response.put("error", "Internal error: " + e.getMessage());
                response.put("success", false);
            }

            // 发送响应
            String jsonResponse = objectMapper.writeValueAsString(response);
            ctx.writeAndFlush(jsonResponse + "\n");
            logger.debug("Sent response: {}", response);
        }

        private Map<String, Object> handleStart(Map<String, Object> request) {
            String nodeId = (String) request.get("nodeId");
            String dataDir = (String) request.get("dataDir");
            int port = ((Number) request.get("port")).intValue();
            boolean isPrimary = (boolean) request.get("isPrimary");
            String replicasStr = (String) request.get("replicas");

            if (runningNodes.containsKey(nodeId)) {
                return Map.of(
                        "success", false,
                        "error", "Node " + nodeId + " is already running"
                );
            }

            // 解析副本列表
            List<String> replicaNodes = new ArrayList<>();
            if (replicasStr != null && !replicasStr.isEmpty()) {
                replicaNodes.addAll(Arrays.asList(replicasStr.split(",")));
            }
            int replicationFactor = replicaNodes.size() + 1;

            logger.info("Starting DataNode: nodeId={}, port={}, isPrimary={}",
                    nodeId, port, isPrimary);

            // 异步启动DataNode
            executor.submit(() -> {
                try {
                    DataNode node = new DataNode(
                            nodeId,
                            dataDir,
                            port,
                            isPrimary,
                            replicaNodes,
                            replicationFactor
                    );

                    node.start();
                    runningNodes.put(nodeId, node);

                    logger.info("DataNode {} started successfully on port {}", nodeId, port);

                    // 添加关闭钩子
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        try {
                            node.stop();
                            runningNodes.remove(nodeId);
                        } catch (Exception e) {
                            logger.error("Error stopping DataNode {} on shutdown: {}", nodeId, e.getMessage());
                        }
                    }));

                } catch (Exception e) {
                    logger.error("Failed to start DataNode {}: {}", nodeId, e.getMessage(), e);
                }
            });

            return Map.of(
                    "success", true,
                    "message", "DataNode start requested",
                    "nodeId", nodeId
            );
        }

        private Map<String, Object> handleStop(Map<String, Object> request) {
            String nodeId = (String) request.get("nodeId");
            DataNode node = runningNodes.get(nodeId);

            if (node == null) {
                return Map.of(
                        "success", false,
                        "error", "Node " + nodeId + " not found"
                );
            }

            logger.info("Stopping DataNode: {}", nodeId);

            executor.submit(() -> {
                try {
                    node.stop();
                    runningNodes.remove(nodeId);
                    logger.info("DataNode {} stopped successfully", nodeId);
                } catch (Exception e) {
                    logger.error("Error stopping DataNode {}: {}", nodeId, e.getMessage(), e);
                }
            });

            return Map.of(
                    "success", true,
                    "message", "DataNode stop requested",
                    "nodeId", nodeId
            );
        }

        private Map<String, Object> handleStatus(Map<String, Object> request) {
            String nodeId = (String) request.get("nodeId");

            if (nodeId == null) {
                // 返回所有节点状态
                List<Map<String, Object>> nodesInfo = new ArrayList<>();

                for (Map.Entry<String, DataNode> entry : runningNodes.entrySet()) {
                    Map<String, Object> info = new HashMap<>();
                    info.put("nodeId", entry.getKey());
                    info.put("running", entry.getValue().isHealthy());
                    nodesInfo.add(info);
                }

                return Map.of(
                        "success", true,
                        "count", runningNodes.size(),
                        "nodes", nodesInfo
                );
            } else {
                // 返回指定节点状态
                DataNode node = runningNodes.get(nodeId);

                if (node == null) {
                    return Map.of(
                            "success", false,
                            "error", "Node " + nodeId + " not found"
                    );
                }

                return Map.of(
                        "success", true,
                        "nodeId", nodeId,
                        "status", node.isHealthy() ? "RUNNING" : "STOPPED"
                );
            }
        }

        private Map<String, Object> handleHealth() {
            return Map.of(
                    "status", "healthy",
                    "service", "DataNode Agent",
                    "timestamp", System.currentTimeMillis(),
                    "runningNodes", runningNodes.size()
            );
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Channel error", cause);
            ctx.close();
        }
    }

    private static void shutdown() {
        logger.info("Shutting down DataNode Agent...");

        // 停止所有DataNode
        for (Map.Entry<String, DataNode> entry : runningNodes.entrySet()) {
            try {
                entry.getValue().stop();
                logger.info("Stopped DataNode: {}", entry.getKey());
            } catch (Exception e) {
                logger.error("Error stopping DataNode {}: {}", entry.getKey(), e.getMessage());
            }
        }
        runningNodes.clear();

        // 关闭线程池
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("DataNode Agent shutdown complete");
    }
}