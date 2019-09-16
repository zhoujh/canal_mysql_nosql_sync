/**
 * canal client  
 * 从canal server 获取 binlog，并写入文件
 * @date 2016-08-13
 * @author liukelin
 * @email 314566990@qq.com
 */
package canal.client;

import java.io.File;
import java.io.FileInputStream;
//import java.io.UnsupportedEncodingException;
//写入文件
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

import canal.client.dto.MessageDto;

public class CanalForwordMain {

	private static String path = System.getProperty("CANAL_CLIENT_CONF");

	public static void main(String args[]) {
		if (path == null) {
			path = ".";
		}
		String confPath = path + "/conf/canal.properties";
		//String host = AddressUtils.getHostIp()
		Config config = new Config();
		//读取配置
		try {
			Properties prop = new Properties();
			InputStream in = new FileInputStream(confPath);
			prop.load(in);
			config.init(prop);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("#=====load conf/canal.properties error!");
		}

		CanalConnector connector = CanalConnectors.newSingleConnector(
				new InetSocketAddress(config.getCanalHost(), config.getCanalPort()), config.getCanalInstance(), "", "");
		//int emptyCount = 0;  
		try {
			connector.connect();
			connector.subscribe(config.getCanalConfSubscribe());
			connector.rollback();

			System.out.println("connect success!\r\n startup...");

			while (true) {
				Message message = connector.getWithoutAck(config.getBatchSize()); // 获取指定数量的数据  
				long batchId = message.getId();
				int size = message.getEntries().size();
				if (batchId == -1 || size == 0) {
					try {
						Thread.sleep(1000); // 等待时间
					} catch (InterruptedException e) {

					}
				} else {
					printEntry(config, message.getEntries());
				}

				connector.ack(batchId); // 提交确认   
			}
		} finally {
			System.out.println("connect error!");
			connector.disconnect();
		}
	}

	private static void printEntry(Config config, List<Entry> entrys) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<String> dataArray = new LinkedList<String>();
		List<MessageDto> messageList = new LinkedList<MessageDto>();
		//循环每行binlog
		for (Entry entry : entrys) {
			if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
					|| entry.getEntryType() == EntryType.TRANSACTIONEND) {
				continue;
			}

			RowChange rowChage = null;
			try {
				rowChage = RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {
				throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
						e);
			}

			//单条 binlog sql
			//EventType eventType = rowChage.getEventType();
			//			String header_str = "{\"binlog\":\"" + entry.getHeader().getLogfileName() + ":"
			//					+ entry.getHeader().getLogfileOffset() + "\"," + "\"db\":\"" + entry.getHeader().getSchemaName()
			//					+ "\"," + "\"table\":\"" + entry.getHeader().getTableName() + "\",";
			//受影响 数据行

			for (RowData rowData : rowChage.getRowDatasList()) {
				MessageDto messageDto = new MessageDto(entry, rowChage, rowData);
				messageList.add(messageDto);
				String msg = JSON.toJSONString(messageDto);
				dataArray.add(msg);
				saveDataLogs(config, msg);
			}
		}
		String[] strArr = dataArray.toArray(new String[] {});

		try {
			Object canalMQ = config.getCanalMQ();
			if (canalMQ.equals("rabbitmq")) {
				RabbitMQSender r = new RabbitMQSender();
				r.pushRabbitmq(config.getMap(), messageList);
				//push_rabbitmq(strArr);
			} else if (canalMQ.equals("redis")) {
				RedisSender r = new RedisSender();
				r.push_redis(config.getMap(), strArr);
			} else if (canalMQ.equals("kafka")) {

			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("push " + config.getCanalMQ() + " error!");
		}
	}

	//save data file
	private static void saveDataLogs(Config config, String row_data) {
		String ts = "yyyyMMdd";
		String canalBinlogFilename = config.getCanalBinlogFilename();
		if (config.getCanalBinlogFilename().equals("y")) {
			ts = "yyyy";
		} else if (canalBinlogFilename.equals("m")) {
			ts = "yyyyMM";
		} else if (canalBinlogFilename.equals("d")) {
			ts = "yyyyMMdd";
		} else if (canalBinlogFilename.equals("h")) {
			ts = "yyyyMMddHH";
		} else if (canalBinlogFilename.equals("i")) {
			ts = "yyyyMMddHHmm";
		} else {

		}
		SimpleDateFormat df2 = new SimpleDateFormat(ts);
		String timeStr2 = df2.format(new Date());
		String filename = config.getDataDir() + File.separatorChar + config.getCanalInstance() + "/binlog_" + timeStr2
				+ ".log";

		FileWriter writer;
		try {
			writer = new FileWriter(filename, true);
			writer.write(row_data + "\r\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("write file error!");
		}
	}

	//check String type
	public static String getEncoding(String str) {
		String[] array = { "Shift_JIS", "GB2312", "ISO-8859-1", "UTF-8", "GBK", "ASCII", "Big5", "Unicode" };
		for (int i = 0; i < array.length; i++) {
			try {
				if (str.equals(new String(str.getBytes(array[i]), array[i]))) {
					String s = array[i];
					if (s.equals("Shift_JIS")) {
						return "unknow";
					}
					return s;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return "other";
	}

}

/**
 * 多线程包concurrent ， Executor 多线程操作
 */
