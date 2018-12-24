package com.yl.mq.topic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.yl.mq.utils.ConnectionUtil;

/**
 * 主题模式
 * @author Administrator
 *
 */
public class Producer {
	 private final static String EXCHANGE_NAME = "topic_exchange";

	    public static void main(String[] args) throws IOException, TimeoutException {
	        //1.获取连接
	        Connection connection = ConnectionUtil.getConnection("localhost", 5674, "/", "guest", "yuelao");
	        //2.声明信道
	        Channel channel = connection.createChannel();
	        //3.声明交换器,类型为direct
	        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
	        //4.定义消息内容
	        String message = "hello rabbitmq";
	        //5.发布消息
	        channel.basicPublish(EXCHANGE_NAME, "update.Name", null, message.getBytes());
	        System.out.println("[x] send'" + message + "'");
	        //6.关闭通道和连接
	        channel.close();
	        connection.close();

	    }

}
