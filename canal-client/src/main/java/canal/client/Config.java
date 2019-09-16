package canal.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
	public String canal_print = "0";

	//canal server
	public String host = "127.0.0.1";
	public int port = 11111;
	public String instance = "example";
	public String confSubscribe = ".*\\..*";
	public int batchSize = 1000; //每次获取数据数量
	public int sleep = 1000; //无数据时等待时间

	//file
	public String canal_binlog_filename = "h"; //保存文件名
	public String data_dir = "data"; //数据保存路径

	//mq
	public String canal_mq; // redis/rabbitmq/kafka

	Properties prop = null;

	public Config(Properties prop) {
		init(prop);
	}

	private int getInt(String key, int d) {
		String value = prop.getProperty(key);
		if (value != null && value != "") {
			return Integer.parseInt(value.trim());
		}
		return d;
	}

	private String getString(String key, String d) {
		String value = prop.getProperty(key);
		if (value != null && value != "") {
			return value.trim();
		}
		return d;
	}

	public Config() {
	}

	public void init(Properties prop) {
		this.prop = prop;
		host = getString("canal.server.host", host);
		port = getInt("canal.server.port", port);
		instance = getString("canal.server.instance", instance);
		batchSize = getInt("canal.batchsize", batchSize);
		confSubscribe = getString("canal.server.subscribe", confSubscribe);
		sleep = getInt("canal.sleep", sleep);
		data_dir = getString("canal.binlog.dir", data_dir);
		canal_binlog_filename = getString("canal.binlog.filename", canal_binlog_filename);
		canal_print = getString("canal.print", canal_print);
		canal_mq = getString("canal.mq", canal_mq);
		System.out.println("#=====host:" + host + ":" + port + "\r\n#=====instance:" + instance + "\r\n");
		File file = new File(data_dir + File.separatorChar + instance);
		if (!file.exists()) {
			file.mkdirs();
		}
	}

	public String getCanalHost() {
		return this.host;
	}

	public int getCanalPort() {
		return this.port;
	}

	public String getCanalInstance() {
		return instance;
	}

	public String getCanalConfSubscribe() {
		return this.confSubscribe;
	}

	public int getBatchSize() {
		return this.batchSize;
	}

	public String getCanalMQ() {
		return this.canal_mq;
	}

	public Properties getMap() {
		return prop;
	}

	public String getCanalBinlogFilename() {
		return this.canal_binlog_filename;
	}

	public Object getCanalPrint() {
		return this.canal_print;
	}

	public String getDataDir() {
		return this.data_dir;
	}

	public static void main(String[] args) {
		String confPath = "E:\\third_code\\canal_mysql_nosql_sync\\canal-client\\conf\\canal.properties";
		Config config = new Config();
		//读取配置
		try {
			Properties prop = new Properties();
			InputStream in = new FileInputStream(confPath);
			prop.load(in);
			config.init(prop);
			System.out.println("--" + config.getMap().get("redis.port") + "--");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("#=====load conf/canal.properties error!");
		}

	}
}
