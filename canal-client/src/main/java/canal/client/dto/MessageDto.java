package canal.client.dto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.google.common.collect.Lists;

public class MessageDto {
	String UPDATE = "UPDATE";
	String DELETE = "DELETE";
	String INSERT = "INSERT";
	String CREATE = "CREATE";

	List<Map<String, Object>> data;
	String database;
	long es;// 1558535842000,
	long id;// 4,
	boolean isDdl;//": false,
	List<Map<String, Object>> old;
	String table;//": "t_t1",
	long ts;//": 1558535842277,
	String type;//": "UPDATE"

	public MessageDto(Entry entry, RowChange rowChage, RowData rowData) {
		this.setType(rowChage.getEventType().name());
		this.setDatabase(entry.getHeader().getSchemaName());
		this.setDdl(rowChage.getIsDdl());
		this.setOld(values(rowData.getBeforeColumnsList()));
		this.setData(values(rowData.getAfterColumnsList()));
		this.setTs(ts);
		this.setTable(entry.getHeader().getTableName());
	}

	private List<Map<String, Object>> values(List<Column> columns) {
		Map<String, Object> columnMap = new HashMap<String, Object>();
		for (Column column : columns) {
			String columnName = column.getName();
			String columnValue = column.getValue();
			columnMap.put(columnName, columnValue);
		}
		return Lists.newArrayList(columnMap);
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public long getEs() {
		return es;
	}

	public void setEs(long es) {
		this.es = es;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isDdl() {
		return isDdl;
	}

	public void setDdl(boolean isDdl) {
		this.isDdl = isDdl;
	}

	public List<Map<String, Object>> getOld() {
		return old;
	}

	public void setOld(List<Map<String, Object>> old) {
		this.old = old;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
