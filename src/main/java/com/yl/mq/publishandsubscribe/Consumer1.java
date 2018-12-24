package com.yl.mq.publishandsubscribe;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.yl.mq.utils.ConnectionUtil;

public class Consumer1 {
	 public static final String QUEUE_NAME = "fanout_queue_1";

	    public static final String EXCHANGE_NAME = "fanout_exchange";

	    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
	        //1.获取连接
	        Connection connection = ConnectionUtil.getConnection("localhost", 5672, "/", "guest", "yuelao");
	        //2.声明信道
	        Channel channel = connection.createChannel();
	        //3.声明交换器
	        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	        //4.绑定队列到交换器
	        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");
	        //同一时刻服务器只会发送一条消息给消费者
	        channel.basicQos(1);

	        //5.定义队列的消费者
	        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
	        //5.监听队列,手动返回完成状态
	        channel.basicConsume(QUEUE_NAME, false, queueingConsumer);
	        //6.获取消息
	        while (true) {
	            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
	            String message = new String(delivery.getBody());
	            System.out.println("[消费者1] received message : '" + message + "'");
	            //休眠10毫秒
	            Thread.sleep(10);
	            //返回确认状态
	            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	        }
	    }
}
