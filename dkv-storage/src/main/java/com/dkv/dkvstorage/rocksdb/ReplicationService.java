package com.dkv.dkvstorage.rocksdb;
import com.dkv.dkvcommon.model.KvMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dkv.dkvcommon.model.KvMessage.Type.DELETE;


public class ReplicationService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

    private final StorageEngine storageEngine;
    private final List<String> replicaNodes;  // 从副本节点地址列表
    private final boolean isPrimary;          // 是否为主节点
    private final EventLoopGroup workerGroup;
    private final int replicationFactor;      // 复制因子（包括主副本）
    private final long replicationTimeout = 5000;  // 复制超时时间（毫秒）

    // 线程池用于异步复制
    private final ExecutorService replicationExecutor;

    public ReplicationService(StorageEngine storageEngine,
                              List<String> replicaNodes,
                              boolean isPrimary,
                              int replicationFactor) {
        this.storageEngine = storageEngine;
        this.replicaNodes = replicaNodes;
        this.isPrimary = isPrimary;
        this.replicationFactor = replicationFactor;
        this.workerGroup = new NioEventLoopGroup(4);
        this.replicationExecutor = Executors.newFixedThreadPool(
                Math.max(2, replicaNodes.size()),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "replication-thread-" + counter.incrementAndGet());
                    }
                }
        );
    }

    /**
     * 同步复制：等待所有从副本确认（强一致性）
     */
    public boolean syncReplicate(KvMessage msg, String key, byte[] value) throws Exception {
        if (!isPrimary) {
            throw new IllegalStateException("Only primary node can initiate replication");
        }

        if (replicaNodes.isEmpty()) {
            return true;  // 没有从副本，直接返回成功
        }

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicaNodes.size());
        KvMessage replicationMsg = new KvMessage(KvMessage.Type.REPLICATION_PUT, key, value);

        if (DELETE.equals(msg.getType())) {
            replicationMsg.setType(DELETE);
        }
        replicationMsg.setReplication(true);
        // 并发发送到所有从副本
        for (String replicaAddr : replicaNodes) {
            replicationExecutor.submit(() -> {
                try {
                    if (sendToReplica(replicaAddr, replicationMsg)) {
                        successCount.incrementAndGet();
                        logger.info("Replication succeeded to {}", replicaAddr);
                    } else {
                        failureCount.incrementAndGet();
                        logger.warn("Replication failed to {}", replicaAddr);
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    logger.error("Replication error to {}: {}", replicaAddr, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有复制操作完成
        boolean completed = latch.await(replicationTimeout, TimeUnit.MILLISECONDS);

        if (!completed) {
            logger.warn("Replication timeout, success: {}, failure: {}",
                    successCount.get(), failureCount.get());
        }

        // 如果大多数副本成功，则认为复制成功
        int success = successCount.get();
        int total = replicaNodes.size();
        return success >= (total + 1) / 2;  // 多数派成功
    }

    /**
     * 异步复制：不等待从副本确认（最终一致性）
     */
    public void asyncReplicate(KvMessage msg,String key, byte[] value) {
        if (!isPrimary || replicaNodes.isEmpty()) {
            return;
        }

        KvMessage replicationMsg = new KvMessage(KvMessage.Type.REPLICATION_PUT, key, value);
        replicationMsg.setReplication(true);

        for (String replicaAddr : replicaNodes) {
            replicationExecutor.submit(() -> {
                try {
                    sendToReplica(replicaAddr, replicationMsg);
                    logger.debug("Async replication sent to {}", replicaAddr);
                } catch (Exception e) {
                    logger.error("Async replication error to {}: {}", replicaAddr, e.getMessage());
                }
            });
        }
    }

    /**
     * 发送数据到副本节点
     */
    private boolean sendToReplica(String replicaAddr, KvMessage message) { // 移除 throws Exception，内部处理
        String[] parts = replicaAddr.split(":");
        if (parts.length != 2) {
            logger.error("Invalid replica address: {}", replicaAddr);
            return false;
        }

        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        // 1. 创建 Future 用于接收结果
        final CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ObjectEncoder());
                        pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                        // FIX: 将 future 传递给 Handler，以便收到响应时通知主线程
                        pipeline.addLast(new ReplicationClientHandler(resultFuture, message.getKey()));
                    }
                });

        Channel channel = null;
        try {
            ChannelFuture connectFuture = b.connect(host, port);

            // 等待连接
            if (!connectFuture.await(3000, TimeUnit.MILLISECONDS)) {
                logger.warn("Connect timeout to {}", replicaAddr);
                return false;
            }

            if (!connectFuture.isSuccess()) {
                logger.warn("Connect failed to {}: {}", replicaAddr, connectFuture.cause().getMessage());
                return false;
            }

            channel = connectFuture.channel();

            // 发送消息
            channel.writeAndFlush(message).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    logger.error("Write failed to {}", replicaAddr, future.cause());
                    // 发送失败直接标记 Future 为失败
                    resultFuture.complete(false);
                }
            });

            // 2. 等待结果（这里等待的是 Handler 里的 complete 调用）
            return resultFuture.get(replicationTimeout, TimeUnit.MILLISECONDS);

        } catch (TimeoutException e) {
            logger.warn("Replica response timeout: {}", replicaAddr);
            return false;
        } catch (Exception e) {
            logger.error("Replication error: ", e);
            return false;
        } finally {
            // 关闭短连接
            if (channel != null) {
                channel.close();
            }
        }
    }
    /**
     * 处理来自主副本的复制请求
     */
    public void handleReplicationRequest(KvMessage message) throws Exception {
        if (!isPrimary) {  // 只有从副本才处理复制请求
            if (message.getType() == KvMessage.Type.REPLICATION_PUT) {
                storageEngine.put(message.getKey(), message.getValue());
                logger.debug("Received replication PUT for key: {}", message.getKey());
            }else if(message.getType() == DELETE){
                storageEngine.delete(message.getKey());
                logger.debug("Received replication delete for key: {}", message.getKey());
            }
        }
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        workerGroup.shutdownGracefully();
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            replicationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Netty客户端处理器
    private static class ReplicationClientHandler extends SimpleChannelInboundHandler<Object> {
        private final CompletableFuture<Boolean> future;
        private final String debugKey;

        public ReplicationClientHandler(CompletableFuture<Boolean> future, String debugKey) {
            this.future = future;
            this.debugKey = debugKey;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof KvMessage) {
                // 收到回复，标记任务成功
                logger.debug("Replica ack received for key: {}", debugKey);
                future.complete(true);
            } else {
                logger.warn("Received unknown message type");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Replication network error", cause);
            // 发生异常，标记任务失败
            future.complete(false);
            ctx.close();
        }
    }
}