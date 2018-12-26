package com.yl.mq.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SimpleMessageProducer {
	
	private static final Logger log = LoggerFactory.getLogger(SimpleMessageProducer.class);
	public final static String QUEUE_NAME = "simple.queue.test";

	public static void main(String[] args) {
		try {
			// 创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
			// 设置RabbitMQ相关信息
			factory.setHost("localhost");
			factory.setUsername("guest");
			factory.setPassword("yuelao");
			factory.setPort(5672);
			// 创建一个新的连接
			Connection connection = factory.newConnection();
			// 从连接中创建通道，这是完成大部分API的地方。
			Channel channel = connection.createChannel();
			// 声明（创建）队列，必须声明队列才能够发送消息，我们可以把消息发送到队列中。
			// 声明一个队列是幂等的 - 只有当它不存在时才会被创建
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			// 消息内容
			String message = "Hello World你好a!";
			channel.confirmSelect();
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			if (channel.waitForConfirms()) {
			    log.info(" [x] Sent '" + message + "'");
			}
			// 关闭通道和连接
			channel.close();
			connection.close();
		} catch (Exception e) {
			log.error("异常",e);
		}

	}
}
