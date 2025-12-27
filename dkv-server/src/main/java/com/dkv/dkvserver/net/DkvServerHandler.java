package com.dkv.dkvserver.net;

import com.dkv.dkvserver.storge.RocksDbEngine;
import com.dkv.dkvserver.storge.StorageEngine;
import kvr.KvRequest;
import kvr.KvResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;
public class DkvServerHandler extends SimpleChannelInboundHandler<KvRequest> {

    private static final StorageEngine engine = new RocksDbEngine();

    static {
        engine.init("./data");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvRequest req) {
        System.out.println("[RPC] Received request: type=" + req.getType() + ", key=" + req.getKey());
        KvResponse resp = new KvResponse();

        try {
            if (req.getType() == 1) { // PUT
                System.out.println("post");
                engine.put(req.getKey(), req.getValue());
                resp.setCode(200);
                resp.setMessage("OK");
            } else if (req.getType() == 2) { // GET
                System.out.println("get");
                byte[] val = engine.get(req.getKey());
                if (val == null) {
                    resp.setCode(404);
                    resp.setMessage("NOT_FOUND");
                } else {
                    resp.setCode(200);
                    resp.setValue(val);
                }
            }
        } catch (Exception e) {
            resp.setCode(500);
            resp.setMessage("ERROR");
        }

        ctx.writeAndFlush(resp);
    }
}
