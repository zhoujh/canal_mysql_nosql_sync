/**
 * RabbitMQSender  
 * 将数据写入到rabbitmq
 * @date 2016-08-27
 * @author liukelin
 * @email 314566990@qq.com
 */
package canal.client;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import canal.client.dto.MessageDto;

/**
 * 每次发送新建了连接，性能较差
 * @author zhoujinhuang
 *
 */
public class RabbitMQSender {

	// 将信息push 到 RabbitMQSender
	public void pushRabbitmq(Properties conf, List<MessageDto> messageList) throws java.io.IOException {
		String rabbitmq_host = conf.getProperty("rabbitmq.host");
		int rabbitmq_port = Integer.parseInt(conf.getProperty("rabbitmq.port"));
		String rabbitmq_user = conf.getProperty("rabbitmq.user");
		String rabbitmq_pass = conf.getProperty("rabbitmq.pass");
		String rabbitmq_queuename = conf.getProperty("rabbitmq.queuename");
		String rabbitmq_exchange = conf.getProperty("rabbitmq.exchange");
		String rabbitmq_vhost = conf.getProperty("rabbitmq.vhost");
		String rabbitmq_durable = conf.getProperty("rabbitmq.durable");
		Boolean durable = false;
		if (rabbitmq_durable.equals("true")) {
			durable = true;
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(rabbitmq_user);
		factory.setHost(rabbitmq_host);
		factory.setPort(rabbitmq_port);
		if (rabbitmq_vhost != null && !"".equals(rabbitmq_vhost)) {
			factory.setVirtualHost(rabbitmq_vhost);
		}
		factory.setPassword(rabbitmq_pass);

		Connection connection = null;
		try {
			connection = factory.newConnection();
		} catch (TimeoutException e1) {
			e1.printStackTrace();
			System.out.println("connection RabbitMQSender error!");
		}
		Channel channel = connection.createChannel();
		for (MessageDto messageDto : messageList) {
			channel.basicPublish(rabbitmq_exchange, messageDto.getDatabase()+":"+messageDto.getTable(), MessageProperties.PERSISTENT_TEXT_PLAIN,
					JSON.toJSONString(messageDto).getBytes());
		}

		try {
			channel.close();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		connection.close();
	}

}
