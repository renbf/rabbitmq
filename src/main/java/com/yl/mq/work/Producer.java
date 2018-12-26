package com.yl.mq.work;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.yl.mq.utils.ConnectionUtil;
/**
 * work模式
 * 一个生产者对应多个消费者,但是只能有一个消费者获得消息!!!
 * @author Administrator
 *
 */
public class Producer {
	 public static final String QUEUE_NAME = "work_queue";

	    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
	        //1.获取连接
	        Connection connection = ConnectionUtil.getConnection("localhost", 5672, "/", "guest", "yuelao");
	        //2.声明信道
	        Channel channel = connection.createChannel();
	        //3.声明(创建)队列
	        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	        //4.定义消息内容,发布多条消息
	        try {
	        	channel.txSelect();
				for (int i = 0; i < 10; i++) {
					String message = "hello rabbitmq " + i;
					//5.发布消息
					channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
					System.out.println("[x] send message is '" + message + "'");
					//6.模拟发送消息延时,便于展示多个消费者竞争接受消息
					Thread.sleep(i * 10);
				} 
				channel.txCommit();
			} catch (Exception e) {
				channel.txRollback();
			}
			//7.关闭信道
	        channel.close();
	        //8.关闭连接
	        connection.close();
	    }
}
