package com.yl.mq.work;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.yl.mq.utils.ConnectionUtil;

public class Consumer2 {
	public static final String QUEUE_NAME = "work_queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = ConnectionUtil.getConnection("localhost", 5672, "/", "guest", "yuelao");
        Channel channel = connection.createChannel();
      //同一时刻服务器只会发送一条消息给消费者
        channel.basicQos(1);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//        channel.basicQos(1);
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME,false,queueingConsumer);
        while (true){
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("[x] received message : '" + message + "'");
            Thread.sleep(30);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
        }
    }
}
