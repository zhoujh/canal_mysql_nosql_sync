/**
 * @author liukelin
 * rpush / lpop 
 */
package canal.client;

import java.util.Properties;

import redis.clients.jedis.Jedis;

public class RedisSender {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//连接本地的 Redis 服务
		Jedis jedis = new Jedis("192.168.179.184");
		System.out.println("Connection to server sucessfully");
		//查看服务是否运行
		System.out.println("Server is running: " + jedis.ping());
	}

	//
	public void push_redis(Properties conf, String[] argv) throws java.io.IOException {
		String host = conf.getProperty("redis.host");
		int port = Integer.parseInt(conf.getProperty("redis.port"));
		String user = conf.getProperty("redis.user");
		String pass = conf.getProperty("redis.pass");
		String queuename = conf.getProperty("redis.queuename");

		Jedis jedis = new Jedis(host, port);
		for (int i = 0; i < argv.length; i++) {
			jedis.rpush(queuename, argv[i]);
		}

	}

}
