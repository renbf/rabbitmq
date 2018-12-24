package com.yl.mq.routing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.yl.mq.utils.ConnectionUtil;

/**
 * 路由模式
 * 生产者将消息发送到direct交换器,在绑定队列和交换器的时候有一个路由key,生产者发送的消息会指定一个路由key,那么消息只会发送到相应key相同的队列,接着监听该队列的消费者消费信息.
 * @author Administrator
 *
 */
public class Producer {
	private final static String EXCHANGE_NAME = "direct_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1.获取连接
        Connection connection = ConnectionUtil.getConnection("localhost", 5672, "/", "guest", "yuelao");
        //2.声明信道
        Channel channel = connection.createChannel();
        //3.声明交换器,类型为direct
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        //4.定义消息内容
        String message = "hello rabbitmq";
        //5.发布消息
        channel.basicPublish(EXCHANGE_NAME, "update", null, message.getBytes());
        System.out.println("[x] send'" + message + "'");
        //6.关闭通道和连接
        channel.close();
        connection.close();

    }
}
