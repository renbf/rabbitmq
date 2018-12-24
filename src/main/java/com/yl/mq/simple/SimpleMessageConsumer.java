package com.yl.mq.simple;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class SimpleMessageConsumer {

	private static final Logger log = LoggerFactory.getLogger(SimpleMessageConsumer.class);
	public final static String QUEUE_NAME = "simple.queue.test";

	public static void main(String[] args) {
		test2();
	}

	public static void test1() {
		try {
			// 创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
			// 设置RabbitMQ地址
			factory.setHost("localhost");
			factory.setUsername("guest");
			factory.setPassword("yuelao");
			factory.setPort(5672);
			// 创建一个新的连接
			Connection connection = factory.newConnection();
			// 创建一个通道
			final Channel channel = connection.createChannel();
			// 声明要关注的队列
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			log.info("Customer Waiting Received messages");
			// DefaultConsumer类实现了Consumer接口，通过传入一个频道，
			// 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
			Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					String message = new String(body, "UTF-8");
					log.info("Customer Received '" + message + "'");
					// 手动确认
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			};
			// 自动回复队列应答 -- RabbitMQ中的消息确认机制
			channel.basicConsume(QUEUE_NAME, false, consumer);
		} catch (Exception e) {
			log.error("异常", e);
		}
	}

	public static void test2() {
		try {
			// 创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
			// 设置RabbitMQ地址
			factory.setHost("localhost");
			factory.setUsername("guest");
			factory.setPassword("yuelao");
			factory.setPort(5672);
			// 创建一个新的连接
			Connection connection = factory.newConnection();
			// 创建一个通道
			Channel channel = connection.createChannel();
			// 声明要关注的队列
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			log.info("Customer Waiting Received messages");
			// 4.定义队列的消费者
			QueueingConsumer consumer = new QueueingConsumer(channel);
			/*
			 * true:表示自动确认,只要消息从队列中获取,无论消费者获取到消息后是否成功消费,都会认为消息成功消费.
			 * false:表示手动确认,消费者获取消息后,服务器会将该消息标记为不可用状态,等待消费者的反馈,
			 * 如果消费者一直没有反馈,那么该消息将一直处于不可用状态,并且服务器会认为该消费者已经挂掉,不会再给其发送消息, 直到该消费者反馈.
			 */
			// 自动回复队列应答 -- RabbitMQ中的消息确认机制
			channel.basicConsume(QUEUE_NAME, false, consumer);

			while (true) {
				Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody(), "UTF-8");
				log.info("Customer Received '" + message + "'");
				// 手动确认
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		} catch (Exception e) {
			log.error("异常", e);
		}
	}
}
