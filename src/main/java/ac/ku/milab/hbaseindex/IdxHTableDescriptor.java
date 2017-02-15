package ac.ku.milab.hbaseindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;

import ac.ku.milab.hbaseindex.util.IdxConstants;
import ac.ku.milab.hbaseindex.util.ValueType;

/* This class is for htable descriptor of index table */
public class IdxHTableDescriptor extends HTableDescriptor {
	private static final Log LOG = LogFactory.getLog(IdxHTableDescriptor.class);

	// list of index column
	private List<IdxColumnQualifier> idxColumns = new ArrayList<IdxColumnQualifier>();

	public IdxHTableDescriptor() {
	}

	public IdxHTableDescriptor(String tableName) {
		super(tableName);
	}

	public IdxHTableDescriptor(byte[] tableName) {
		super(tableName);
	}

	/**
	 * @param qualifier
	 *            index column's name
	 * @return
	 */

	public void addIndexColumn(String qualifier) throws IllegalArgumentException {
		// if index column is null, error
		if (qualifier == null) {
			LOG.info("column name is null");
			throw new IllegalArgumentException();
		}

		// if length of index column's name is more than limit, error
		if (qualifier.length() > IdxConstants.MAX_INDEX_NAME_LENGTH) {
			LOG.info("column name is too long");
			throw new IllegalArgumentException();
		}

		// if index column already exists, error
		for (IdxColumnQualifier idxColumn : idxColumns) {
			if (idxColumn.getQualifierName().equals(qualifier)) {
				LOG.info("index column already exists");
				throw new IllegalArgumentException();
			}
		}

		// add index column
		IdxColumnQualifier idxColumn = new IdxColumnQualifier(qualifier, ValueType.String);
		this.idxColumns.add(idxColumn);
	}

	/**
	 * @param
	 * @return list of index column qualifier
	 */

	public List<IdxColumnQualifier> getIndexColumns() {
		return new ArrayList<IdxColumnQualifier>(this.idxColumns);
	}

	/**
	 * @param
	 * @return number of index column
	 */

	public int getIndexColumnCount() {
		return this.idxColumns.size();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(this.idxColumns.size());
		for (IdxColumnQualifier qual : idxColumns) {
			qual.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		super.readFields(in);
		int indexSize = in.readInt();
		idxColumns.clear();
		for (int i = 0; i < indexSize; i++) {
			IdxColumnQualifier qual = new IdxColumnQualifier();
			qual.readFields(in);
			this.idxColumns.add(qual);
		}

	}
}
