package com.dkv.dkvclient.client;

import com.dkv.dkvcommon.model.KvMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ClientHandler extends SimpleChannelInboundHandler<KvMessage> {

    private KvMessage response;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvMessage msg) {
        this.response = msg;
    }

    public KvMessage getResponse() {
        return response;
    }
}
