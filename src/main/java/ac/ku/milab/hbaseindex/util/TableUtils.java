package ac.ku.milab.hbaseindex.util;
import java.util.Collection;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

/* This class is for Utility of table functions */
public class TableUtils {

	/**
	 * @param tableName
	 * @return whether this table is meta table or root table
	 */

	public static boolean isSystemTable(byte[] tableName) {
		TableName tName = TableName.valueOf(tableName);

		if(tName.isSystemTable()){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * @param tableName
	 * @return whether this table is index table
	 */

	public static boolean isIndexTable(byte[] tableName) {
		String strTableName = Bytes.toString(tableName);

		if (strTableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @param tableName
	 * @return whether this table is user table
	 */

	public static boolean isUserTable(byte[] tableName) {
		boolean isUserTable = !(isSystemTable(tableName) || isIndexTable(tableName));

		return isUserTable;
	}

	/**
	 * @param indexTableName
	 * @return user table name of index table
	 */

	public static String extractTableName(String indexTableName) {
		int tableNameLength = indexTableName.length() - IdxConstants.IDX_TABLE_SUFFIX.length();
		return indexTableName.substring(0, tableNameLength);
	}

	/**
	 * @param TableName
	 * @return index table name of user table
	 */

	public static String getIndexTableName(String tableName) {
		return tableName + IdxConstants.IDX_TABLE_SUFFIX;
	}

	/**
	 * @param tableName
	 *            user table name
	 * @param startKey
	 *            region's start key
	 * @param regionServer
	 *            region server having region
	 * @return index table name of user table
	 */
//
//	public static HRegion getIndexTableRegion(String tableName, byte[] startKey, HRegionServer regionServer) {
//		String indexTableName = getIndexTableName(tableName);
//		Collection<HRegion> idxTableRegions = regionServer.getOnlineRegions(Bytes.toBytes(indexTableName));
//		for (HRegion idxTableRegion : idxTableRegions) {
//			if (Bytes.equals(idxTableRegion.getStartKey(), startKey)) {
//				return idxTableRegion;
//			}
//		}
//		return null;
//	}
}
