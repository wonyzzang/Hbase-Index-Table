package ac.ku.milab.hbaseindex;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnFilterWrapper extends SingleColumnValueFilter {
	
	private static final Log LOG = LogFactory.getLog(SingleColumnFilterWrapper.class.getName());
	
	private byte[] targetValue = null;

	public SingleColumnFilterWrapper(byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value) {
		super(family, qualifier, compareOp, value);
		targetValue = Bytes.copy(value);
		// TODO Auto-generated constructor stub
	}
	
	public byte[] getTargetValue(){
		return Bytes.copy(targetValue);
	}
	
	@Override
	public byte[] toByteArray(){
		byte[] byteArray = super.toByteArray();
		byteArray = Bytes.add(byteArray, targetValue);
		LOG.info("toByteArray");
		return byteArray;
	}
	
	public static SingleColumnFilterWrapper parseFrom(byte[] bytes) throws DeserializationException{
		SingleColumnFilterWrapper wrapper = null;
		byte[] byteFilter = Bytes.copy(bytes, 0, bytes.length-8);
		byte[] byteValue = Bytes.copy(bytes,bytes.length-8, bytes.length);
		
		SingleColumnValueFilter filter = SingleColumnValueFilter.parseFrom(byteFilter);
		
		wrapper = (SingleColumnFilterWrapper) filter;
		wrapper.targetValue = byteValue;
		LOG.info("parseFrom");
		return wrapper;
	}
	
//
//	public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
//		int num = filterArguments.size();
//		byte[] value = filterArguments.get(num-1);
//		byte[] tmp;
//		byte[] target1;
//		byte[] target2;
//		byte[] target3;
//
//		switch (num) {
//		case 1:
//			tmp = filterArguments.get(0);
//			target1 = new byte[tmp.length - 2];
//			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);
//			return new CarNumFilter(target1);
//		case 2:
//			tmp = filterArguments.get(0);
//			target1 = new byte[tmp.length - 2];
//			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);
//
//			tmp = filterArguments.get(1);
//			target2 = new byte[tmp.length - 2];
//			Bytes.putBytes(target2, 0, tmp, 1, tmp.length - 2);
//			return new CarNumFilter(target1, target2);
//		case 3:
//			tmp = filterArguments.get(0);
//			target1 = new byte[tmp.length - 2];
//			Bytes.putBytes(target1, 0, tmp, 1, tmp.length - 2);
//
//			tmp = filterArguments.get(1);
//			target2 = new byte[tmp.length - 2];
//			Bytes.putBytes(target2, 0, tmp, 1, tmp.length - 2);
//
//			tmp = filterArguments.get(2);
//			target3 = new byte[tmp.length - 2];
//			Bytes.putBytes(target3, 0, tmp, 1, tmp.length - 2);
//			return new CarNumFilter(target1, target2, target3);
//		default:
//			return null;
//		}
//
//	}

}
