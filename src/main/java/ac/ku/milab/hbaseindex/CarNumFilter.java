package ac.ku.milab.hbaseindex;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class CarNumFilter extends FilterBase {

	private static final Log LOG = LogFactory.getLog(CarNumFilter.class.getName());

	private static final long LONG_NULL = 0;
	private static final byte[] BYTE_NULL = Bytes.toBytes(LONG_NULL);

	private byte[] carNum = null;
	private byte[] startTime = BYTE_NULL;
	private byte[] endTime = BYTE_NULL;

	private boolean filterRow = false;

	private boolean isRangeQuery = false;

	public CarNumFilter() {
		super();
	}

	public CarNumFilter(String sCarNum) {
		this.isRangeQuery = false;
		this.carNum = Bytes.toBytes(sCarNum);
	}

	public CarNumFilter(String sCarNum, long time) {
		this.isRangeQuery = false;
		this.carNum = Bytes.toBytes(sCarNum);
		this.startTime = Bytes.toBytes(time);
	}

	public CarNumFilter(String sCarNum, long startTime, long endTime) {
		this.isRangeQuery = true;
		this.carNum = Bytes.toBytes(sCarNum);
		this.startTime = Bytes.toBytes(startTime);
		this.endTime = Bytes.toBytes(endTime);
	}

	public CarNumFilter(String sCarNum, long startTime, long endTime, boolean filterRow) {
		this.isRangeQuery = true;
		this.carNum = Bytes.toBytes(sCarNum);
		this.startTime = Bytes.toBytes(startTime);
		this.endTime = Bytes.toBytes(endTime);
		this.filterRow = filterRow;
	}

	public CarNumFilter(byte[] carNum) {
		this.isRangeQuery = false;
		this.carNum = carNum;
	}

	public CarNumFilter(byte[] carNum, byte[] time) {
		this.isRangeQuery = false;
		this.carNum = carNum;
		this.startTime = time;
	}

	public CarNumFilter(byte[] carNum, byte[] time, boolean filterRow) {
		this.isRangeQuery = false;
		this.carNum = carNum;
		this.startTime = time;
		this.filterRow = filterRow;
	}

	public CarNumFilter(byte[] carNum, byte[] startTime, byte[] endTime) {
		this.isRangeQuery = true;
		this.carNum = carNum;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public CarNumFilter(byte[] carNum, byte[] startTime, byte[] endTime, boolean filterRow) {
		this.isRangeQuery = true;
		this.carNum = carNum;
		this.startTime = startTime;
		this.endTime = endTime;
		this.filterRow = filterRow;
	}

	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
		byte[] rowkey = Bytes.copy(buffer, offset, length);

		byte[] carNum = Bytes.copy(rowkey, 0, 9);
		LOG.info("filter : isRangeQuery" + isRangeQuery);
		if (this.isRangeQuery) {
			byte[] time = Bytes.copy(rowkey, 10, 8);
			if (Bytes.compareTo(time, this.startTime) == 1 && Bytes.compareTo(time, this.endTime) == -1) {
				return false;
			} else {
				return true;
			}

		} else {
			if (Bytes.equals(this.startTime, BYTE_NULL)) {
				if (Bytes.equals(carNum, this.carNum)) {
					return false;
				} else {
					return true;
				}
			} else {
				byte[] time = Bytes.copy(rowkey, 10, 8);
				if (Bytes.equals(carNum, this.carNum) && Bytes.equals(time, this.startTime)) {
					return false;
				} else {
					return true;
				}
			}
		}

	}

	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		return filterRow;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.filterRow = false;
	}

	@Override
	public ReturnCode filterKeyValue(Cell c) throws IOException {
		return ReturnCode.INCLUDE;
	}

	public void setFilterRow(boolean filterRow) {
		this.filterRow = filterRow;
	}

	@Override
	public byte[] toByteArray() {
		byte[] array = new byte[0];
		array = Bytes.add(array, Bytes.toBytes(this.isRangeQuery));
		array = Bytes.add(array, this.carNum);
		array = Bytes.add(array, this.startTime);
		array = Bytes.add(array, this.endTime);
		array = Bytes.add(array, Bytes.toBytes(this.filterRow));
		return array;
	}

	public static CarNumFilter parseFrom(byte[] bytes) throws DeserializationException {
		CarNumFilter filter = null;

		boolean isRangeQuery = Bytes.toBoolean(Bytes.copy(bytes, 0, 1));

		byte[] carNum = Bytes.copy(bytes, 1, 9);
		byte[] startTime = Bytes.copy(bytes, 10, 8);
		boolean filterRow = Bytes.toBoolean(Bytes.copy(bytes, 26, 1));

		if (isRangeQuery) {
			byte[] endTime = Bytes.copy(bytes, 18, 8);
			filter = new CarNumFilter(carNum, startTime, endTime, filterRow);
		} else {
			filter = new CarNumFilter(carNum, startTime, filterRow);
		}

		return filter;
	}

	public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
		int num = filterArguments.size();
		byte[] tmp;
		byte[] target1;
		byte[] target2;
		byte[] target3;

		switch (num) {
		case 1:
			tmp = filterArguments.get(0);
			target1 = new byte[tmp.length - 2];
			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);
			return new CarNumFilter(target1);
		case 2:
			tmp = filterArguments.get(0);
			target1 = new byte[tmp.length - 2];
			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);

			tmp = filterArguments.get(1);
			target2 = new byte[tmp.length - 2];
			Bytes.putBytes(target2, 0, tmp, 1, tmp.length - 2);
			return new CarNumFilter(target1, target2);
		case 3:
			tmp = filterArguments.get(0);
			target1 = new byte[tmp.length - 2];
			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);

			tmp = filterArguments.get(1);
			target2 = new byte[tmp.length - 2];
			Bytes.putBytes(target2, 0, tmp, 1, tmp.length - 2);

			tmp = filterArguments.get(2);
			target3 = new byte[tmp.length - 2];
			Bytes.putBytes(target3, 0, tmp, 1, tmp.length - 2);
			return new CarNumFilter(target1, target2, target3);
		default:
			return null;
		}

	}
}
