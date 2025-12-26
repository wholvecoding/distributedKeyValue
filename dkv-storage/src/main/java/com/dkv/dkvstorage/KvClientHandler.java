package com.dkv.dkvstorage;

import com.dkv.dkvstorage.rocksdb.KvMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.CompletableFuture;

public class KvClientHandler extends SimpleChannelInboundHandler<KvMessage> {

    private final CompletableFuture<KvMessage> future;

    public KvClientHandler(CompletableFuture<KvMessage> future) {
        this.future = future;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvMessage msg) {
        future.complete(msg);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        future.completeExceptionally(cause);
        ctx.close();
    }
}
