package com.dkv.dkvclient;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kvr.KvResponse;

import java.util.concurrent.CountDownLatch;

public class ClientHandler extends SimpleChannelInboundHandler<KvResponse> {

    private static volatile KvResponse response;
    private static CountDownLatch latch;

    public static void setWaiting(boolean wait) {
        if (wait) {
            latch = new CountDownLatch(1);
        }
    }

    public static KvResponse waitResult() {
        try {
            latch.await();  // 等待 RPC 响应
            return response; // 直接返回 KvResponse
        } catch (InterruptedException e) {
            return KvResponse.error("Interrupted while waiting for response");
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvResponse msg) {
        response = msg;
        latch.countDown();
    }
}
