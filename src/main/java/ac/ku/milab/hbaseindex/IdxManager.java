package ac.ku.milab.hbaseindex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdxManager {

	// index manager
	private static IdxManager manager = new IdxManager();

	// map of table and index
	private Map<String, List<IdxColumnQualifier>> tableVsIndex = new ConcurrentHashMap<String, List<IdxColumnQualifier>>();

	private IdxManager() {
	}

	/**
	 * @return Index manager
	 */

	public static IdxManager getInstance() {
		return manager;
	}

	/**
	 * @param tableName
	 * @param indexList
	 * @return
	 */

	public void addIndexForTable(String tableName, List<IdxColumnQualifier> indexList) {
		this.tableVsIndex.put(tableName, indexList);
	}

	/**
	 * @param tableName
	 * @return list of table's index column
	 */

	public List<IdxColumnQualifier> getIndexOfTable(String tableName) {
		return this.tableVsIndex.get(tableName);
	}

	/**
	 * @param tableName
	 * @return
	 */

	public void removeIndex(String tableName) {
		this.tableVsIndex.remove(tableName);
	}

	/**
	 * @param tableName
	 * @param columnName
	 * @return IdxColumnQualifier whose name is columnName
	 */

	public IdxColumnQualifier getIndexColumn(String tableName, String columnName) {
		List<IdxColumnQualifier> idxColumns = this.tableVsIndex.get(tableName);
		for (IdxColumnQualifier qual : idxColumns) {
			if (qual.getQualifierName().equals(columnName)) {
				return qual;
			}
		}
		return null;
	}

}
