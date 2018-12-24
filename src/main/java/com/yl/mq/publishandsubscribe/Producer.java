package com.yl.mq.publishandsubscribe;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.yl.mq.utils.ConnectionUtil;

/**
 * 发布、订阅模式，发送端发送广播消息，多个接收端接收。
 * 一个生产者将消息首先发送到交换器,交换器绑定多个队列,然后被监听该队列的消费者所接收并消费.
*在RabbitMQ中,交换器主要有四种类型:direct,fanout,topic,headers,这里的交换器是fanout.
 * @author Administrator
 *
 */
public class Producer {
	private final static String EXCHANGE_NAME = "fanout_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1.获取连接
        Connection connection = ConnectionUtil.getConnection("localhost", 5672, "/", "guest", "yuelao");
        //2.声明信道
        Channel channel = connection.createChannel();
        //3.声明交换器
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        //4.定义消息内容
        String message = "hello rabbitmq";
        //5.发布消息
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
        System.out.println("[x] send'" + message + "'");
        //6.关闭通道和连接
        channel.close();
        connection.close();

    }
}
