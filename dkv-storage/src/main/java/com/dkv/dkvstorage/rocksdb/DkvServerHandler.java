package com.dkv.dkvstorage.rocksdb;
// DkvServerHandler.java
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DkvServerHandler extends SimpleChannelInboundHandler<KvMessage> {
    private static final Logger logger = LoggerFactory.getLogger(DkvServerHandler.class);

    private final StorageEngine storageEngine;
    private final ReplicationService replicationService;
    private final boolean isPrimary;

    public DkvServerHandler(StorageEngine storageEngine,
                            ReplicationService replicationService,
                            boolean isPrimary) {
        this.storageEngine = storageEngine;
        this.replicationService = replicationService;
        this.isPrimary = isPrimary;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvMessage msg) throws Exception {
        logger.debug("Received message: {}", msg);

        KvMessage response = new KvMessage(KvMessage.Type.RESPONSE, msg.getKey(), null);
        response.setRequestId(msg.getRequestId());

        try {
            switch (msg.getType()) {
                case PUT:
                    handlePut(ctx, msg, response);
                    break;

                case GET:
                    handleGet(ctx, msg, response);
                    break;

                case DELETE:
                    handleDelete(ctx, msg, response);
                    break;

                case REPLICATION_PUT:
                    // 处理复制请求
                    replicationService.handleReplicationRequest(msg);
                    response.setStatusCode(200);
                    response.setMessage("Replication OK");
                    break;

                default:
                    response.setStatusCode(400);
                    response.setMessage("Unknown operation type");
            }
        } catch (Exception e) {
            logger.error("Error handling request", e);
            response.setStatusCode(500);
            response.setMessage("Internal server error: " + e.getMessage());
        }

        ctx.writeAndFlush(response);
    }

    private void handlePut(ChannelHandlerContext ctx, KvMessage msg, KvMessage response) throws Exception {
        String key = msg.getKey();
        byte[] value = msg.getValue();

        if (key == null || value == null) {
            response.setStatusCode(400);
            response.setMessage("Key and value cannot be null");
            return;
        }

        // 写入本地存储
        storageEngine.put(key, value);

        // 如果是主节点，需要复制到从节点
        if (isPrimary && !msg.isReplication()) {
            boolean replicationSuccess = replicationService.syncReplicate(key, value);

            if (replicationSuccess) {
                response.setStatusCode(200);
                response.setMessage("Put successful with replication");
            } else {
                // 复制失败，可能需要回滚或记录警告
                response.setStatusCode(202);  // Accepted但复制不完全
                response.setMessage("Put successful but replication incomplete");
                logger.warn("Replication incomplete for key: {}", key);
            }
        } else {
            response.setStatusCode(200);
            response.setMessage("Put successful");
        }
    }

    private void handleGet(ChannelHandlerContext ctx, KvMessage msg, KvMessage response) throws Exception {
        String key = msg.getKey();

        if (key == null) {
            response.setStatusCode(400);
            response.setMessage("Key cannot be null");
            return;
        }

        byte[] value = storageEngine.get(key);

        if (value != null) {
            response.setStatusCode(200);
            response.setValue(value);
            response.setMessage("Get successful");
        } else {
            response.setStatusCode(404);
            response.setMessage("Key not found");
        }
    }

    private void handleDelete(ChannelHandlerContext ctx, KvMessage msg, KvMessage response) throws Exception {
        String key = msg.getKey();

        if (key == null) {
            response.setStatusCode(400);
            response.setMessage("Key cannot be null");
            return;
        }

        storageEngine.delete(key);

        // 如果是主节点，需要复制删除操作到从节点
        if (isPrimary && !msg.isReplication()) {
            // 注意：对于删除操作，我们需要特殊的处理
            replicationService.asyncReplicate(key, null);  // 简化处理
        }

        response.setStatusCode(200);
        response.setMessage("Delete successful");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Channel error", cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Client connected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("Client disconnected: {}", ctx.channel().remoteAddress());
    }
}
