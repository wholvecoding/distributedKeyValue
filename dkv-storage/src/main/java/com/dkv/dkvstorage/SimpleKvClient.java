package com.dkv.dkvstorage;


import com.dkv.dkvcommon.model.KvMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;


import java.util.concurrent.CompletableFuture;

public class SimpleKvClient {

    private final String host;
    private final int port;

    public SimpleKvClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public KvMessage send(KvMessage request) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        CompletableFuture<KvMessage> future = new CompletableFuture<>();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new ObjectEncoder());
                            p.addLast(new ObjectDecoder(
                                    ClassResolvers.cacheDisabled(null)));
                            p.addLast(new KvClientHandler(future));
                        }
                    });

            Channel ch = b.connect(host, port).sync().channel();
            ch.writeAndFlush(request).sync();

            return future.get();
        } finally {
            group.shutdownGracefully();
        }
    }
}
