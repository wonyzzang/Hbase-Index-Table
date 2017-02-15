package ac.ku.milab.hbaseindex.util;

import org.apache.hadoop.hbase.util.Bytes;

/* This class is for constants */
public class IdxConstants {

	// name suffix of index table
	public static final String IDX_TABLE_SUFFIX = "_idx";

	// index table's column family name
	public static final byte[] IDX_FAMILY = Bytes.toBytes("IND");

	// index table's column qualifier name
	public static final byte[] IDX_QUALIFIER = Bytes.toBytes("IND");

	// index table's column value
	public static final byte[] IDX_VALUE = Bytes.toBytes("0");

	// max index column name length
	public static final int MAX_INDEX_NAME_LENGTH = 15;

}
