package com.dkv.dkvstorage.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class NettyAgentClient {

    private final String host;
    private final int port;

    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    private volatile Channel channel;

    private final BlockingQueue<Map<String, Object>> responseQueue =
            new LinkedBlockingQueue<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public NettyAgentClient(String host, int port) {
        this.host = host;
        this.port = port;

        this.group = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new StringDecoder());
                        p.addLast(new StringEncoder());
                        p.addLast(new ClientHandler(responseQueue));
                    }
                });
        connect();

    }

    private synchronized void connect() {
        try {
            if (channel != null && channel.isActive()) {
                return;
            }

            ChannelFuture future = bootstrap.connect(host, port).sync();
            this.channel = future.channel();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Netty connect interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("Netty connect failed", e);
        }
    }

    /**
     * Map 请求 → Map 响应（同步）
     */
    public Map<String, Object> sendAndReceive(Map<String, Object> request) {
        ensureConnected();

        // ⭐ 防止读到上一次响应
        responseQueue.clear();

        try {
            String json = MAPPER.writeValueAsString(request);
            channel.writeAndFlush(json);

            Map<String, Object> resp =
                    responseQueue.poll(5, TimeUnit.SECONDS);

            if (resp == null) {
                throw new RuntimeException("Netty response timeout");
            }
            return resp;

        } catch (Exception e) {
            throw new RuntimeException("Netty send failed", e);
        }
    }

    private void ensureConnected() {
        if (channel == null || !channel.isActive()) {
            connect();
        }
    }

    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * Handler：JSON → Map
     */
    private static class ClientHandler
            extends SimpleChannelInboundHandler<String> {

        private final BlockingQueue<Map<String, Object>> responseQueue;

        ClientHandler(BlockingQueue<Map<String, Object>> responseQueue) {
            this.responseQueue = responseQueue;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg)
                throws Exception {

            Map<String, Object> map =
                    MAPPER.readValue(msg,
                            new TypeReference<Map<String, Object>>() {});
            responseQueue.offer(map);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}



//package com.dkv.dkvstorage.agent;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.netty.bootstrap.Bootstrap;
//import io.netty.channel.*;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.string.StringDecoder;
//import io.netty.handler.codec.string.StringEncoder;
//import io.netty.handler.timeout.ReadTimeoutHandler;
//import io.netty.handler.timeout.WriteTimeoutHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Map;
//import java.util.concurrent.*;
//
//public class NettyAgentClient {
//    private static final Logger logger = LoggerFactory.getLogger(NettyAgentClient.class);
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//
//    private final String host;
//    private final int port;
//    private final int connectTimeout;
//    private final int requestTimeout;
//
//    private EventLoopGroup group;
//    private Channel channel;
//    private Bootstrap bootstrap;
//    private final ResponseHandler responseHandler;
//
//    public NettyAgentClient(String host, int port) {
//        this(host, port, 5000, 10000);
//
//    }
//
//    public NettyAgentClient(String host, int port, int connectTimeout, int requestTimeout) {
//        System.out.println(host);
//        System.out.println(port);
//        this.host = host;
//        this.port = port;
//        this.connectTimeout = connectTimeout;
//        this.requestTimeout = requestTimeout;
//        System.out.println("new ResponseHandler");
//        this.responseHandler = new ResponseHandler();
//    }
//
//    /**
//     * 连接到Agent服务器
//     */
//    public void connect() throws Exception {
//        if (channel != null && channel.isActive()) {
//            logger.debug("Already connected to {}:{}", host, port);
//            return;
//        }
//
//        logger.info("Connecting to Agent at {}:{}", host, port);
//
//        group = new NioEventLoopGroup(1);
//
//        try {
//            bootstrap = new Bootstrap();
//            bootstrap.group(group)
//                    .channel(NioSocketChannel.class)
//                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
//                    .option(ChannelOption.TCP_NODELAY, true)
//                    .option(ChannelOption.SO_KEEPALIVE, true)
//                    .handler(new ChannelInitializer<SocketChannel>() {
//                        @Override
//                        protected void initChannel(SocketChannel ch) {
//                            ChannelPipeline pipeline = ch.pipeline();
//
//                            // 超时处理器
//                            pipeline.addLast(new ReadTimeoutHandler(requestTimeout / 1000));
//                            pipeline.addLast(new WriteTimeoutHandler(requestTimeout / 1000));
//
//                            // 字符串编解码器
//                            pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
//                            pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));
////
//                            // 响应处理器
//                            pipeline.addLast(responseHandler);
//                        }
//                    });
//
//            ChannelFuture future = bootstrap.connect(host, port).sync();
//            channel = future.channel();
//
//            logger.info("Connected to Agent at {}:{}", host, port);
//
//        } catch (Exception e) {
//            logger.error("Failed to connect to Agent {}:{} - {}", host, port, e.getMessage());
//            if (group != null) {
//                group.shutdownGracefully();
//            }
//            throw e;
//        }
//    }
//
//    /**
//     * 发送请求到Agent
//     */
//    public Map<String, Object> sendRequest(Map<String, Object> request) throws Exception {
//        if (channel == null || !channel.isActive()) {
//            connect();
//        }
//
//        try {
//            // 将请求转换为JSON字符串
//            String requestJson = objectMapper.writeValueAsString(request);
//
//            // 发送请求
//            ChannelFuture writeFuture = channel.writeAndFlush(requestJson + "\n").sync();
//            if (!writeFuture.isSuccess()) {
//                throw new Exception("Failed to send request: " + writeFuture.cause().getMessage());
//            }
//
//            logger.debug("Sent request to Agent: {}", request);
//
//            // 等待响应
//            String responseJson = responseHandler.getResponse(requestTimeout, TimeUnit.MILLISECONDS);
//
//            if (responseJson == null) {
//                throw new TimeoutException("Request timeout after " + requestTimeout + "ms");
//            }
//
//            // 解析响应
//            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
//            logger.debug("Received response from Agent: {}", response);
//
//            return response;
//
//        } catch (Exception e) {
//            logger.error("Error sending request to Agent {}:{} - {}", host, port, e.getMessage());
//            // 连接可能已断开，下次重新连接
//            disconnect();
//            throw e;
//        }
//    }
//
//    /**
//     * 断开连接
//     */
//    public void disconnect() {
//        logger.info("Disconnecting from Agent at {}:{}", host, port);
//
//        if (channel != null) {
//            channel.close().awaitUninterruptibly();
//            channel = null;
//        }
//
//        if (group != null) {
//            group.shutdownGracefully();
//            group = null;
//        }
//
//        logger.info("Disconnected from Agent at {}:{}", host, port);
//    }
//
//    /**
//     * 检查是否连接
//     */
//    public boolean isConnected() {
//        return channel != null && channel.isActive();
//    }
//
//    /**
//     * 响应处理器
//     */
//    private static class ResponseHandler extends SimpleChannelInboundHandler<String> {
//        private final BlockingQueue<String> responseQueue = new LinkedBlockingQueue<>();
//
//        @Override
//        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
//            logger.debug("Received raw response: {}", msg);
//            responseQueue.offer(msg);
//        }
//
//        public String getResponse(long timeout, TimeUnit unit) throws InterruptedException {
//            return responseQueue.poll(timeout, unit);
//        }
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//            logger.error("Error in ResponseHandler", cause);
//            ctx.close();
//        }
//    }
//}