package com.dkv.dkvstorage.rocksdb;
// DkvServerHandler.java
import com.dkv.dkvcommon.model.KvMessage;
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
    protected void channelRead0(ChannelHandlerContext ctx, KvMessage msg) {
        logger.debug("Received message type: {}, key: {}, isReplication: {}",
                msg.getType(), msg.getKey(), msg.isReplication());

        KvMessage response = new KvMessage(KvMessage.Type.RESPONSE, msg.getKey(), null);
        response.setRequestId(msg.getRequestId());

        try {
            switch (msg.getType()) {
                case PUT:
                    // 处理客户端直接发来的 PUT
                    handlePut(ctx, msg, response);
                    break;

                case REPLICATION_PUT:
                    // 【关键修复】直接在这里写入，不要委托给 ReplicationService
                    // 只要收到这个消息，就意味着这是一个强制写入指令
                    storageEngine.put(msg.getKey(), msg.getValue());

                    logger.debug("Replica data written for key: {}", msg.getKey());
                    response.setStatusCode(200);
                    response.setMessage("Replication OK");
                    break;

                case GET:
                    handleGet(ctx, msg, response);
                    break;

                case DELETE:
                    handleDelete(ctx, msg, response);
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

        // 1. 如果我是从节点，且收到了普通的 PUT 请求（非 REPLICATION_PUT）
        // 这里需要根据你的架构决定：是拒绝写入？还是允许从节点写入？
        // 通常建议从节点只读，或者转发给主节点。
        // 这里假设暂允许写入，或者你确保客户端只会把 PUT 发给主节点。

        // 写入本地
        storageEngine.put(key, value);
        logger.info("当前是否为主节点: "+this.isPrimary);
        // 2. 只有【主节点】且【非复制消息】才触发同步
        if (this.isPrimary && !msg.isReplication()) {
            // 调用同步复制
            boolean success = replicationService.syncReplicate(msg, key, value);
            if (success) {
                response.setStatusCode(200);
                response.setMessage("Put success (Replicated)");
            } else {
                response.setStatusCode(202); // 202 Accepted: 已写入主，但复制失败
                response.setMessage("Put success (Replication failed)");
                logger.warn("Replication failed for key: {}", key);
            }
        } else {
            // 从节点直接返回成功（或者单机模式）
            response.setStatusCode(200);
            response.setMessage("Put success (Local)");
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


        // 如果是主节点，需要复制到从节点
        if (isPrimary && !msg.isReplication()) {
            boolean replicationSuccess = replicationService.syncReplicate(msg, key, null);

            if (replicationSuccess) {
                response.setStatusCode(200);
                response.setMessage("Delete successful with replication");
            } else {
                // 复制失败，可能需要回滚或记录警告
                response.setStatusCode(202);  // Accepted但复制不完全
                response.setMessage("Delete successful but replication incomplete");
                logger.warn("DELETE Replication incomplete for key: {}", key);
            }
        } else {
            response.setStatusCode(200);
            response.setMessage("Put successful");
        }

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
