package ac.ku.milab.hbaseindex;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

public class IdxFilter extends FilterBase {
	
	private byte[] qualifier = null;
	private byte[] qualNum = null;
	private byte[] value = null;
	private boolean filterRow = false;
	
	private static final Log LOG = LogFactory.getLog(IdxFilter.class.getName());
	
	public IdxFilter() {
		super();
	}
	
	public IdxFilter(byte[] qualifier, byte[] value){
		this.qualifier = qualifier;
		this.qualNum = getQualNumber(qualifier);
		this.value = value;
	}
	
	public IdxFilter(byte[] qualifier, byte[] value, boolean filterRow){
		this.qualifier = qualifier;
		this.qualNum = getQualNumber(qualifier);
		this.value = value;
		this.filterRow = filterRow;
	}
	
	private byte[] getQualNumber(byte[] qual){
		int qLength = Bytes.toBytes("q").length;

		return Bytes.copy(qual,qLength, qual.length-qLength);
	}
	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		byte[] rowkey = Bytes.copy(buffer, offset, length);
		
		//LOG.info("buffer is : "+Bytes.toString(rowkey));
		byte[] carNum = Bytes.copy(rowkey, 0, 9);
		byte[] time = Bytes.copy(rowkey, 10, 8);
		String rowKey = Bytes.toString(rowkey);
		byte[] query = Bytes.add(Bytes.toBytes("idx"),this.qualNum,this.value);
		query = Bytes.add(query, Bytes.toBytes("2v"));
		//String qualValue = rowKey.split("idx")[1];
		//String val = Bytes.toString(this.value);
		//LOG.info("qual value is : "+ qualValue);
		
//		if(rowKey.contains(val)){
//			return false;
//		}else{
//			return true;
//		}
		
		if(Bytes.contains(rowkey, query)){
			return false;
		}else{
			return true;
		}
		
		//return super.filterRowKey(buffer, offset, length);
		
	}
	
	
	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		return filterRow;
	}
	
//	@Override
//	public boolean filterAllRemaining() {
//		// TODO Auto-generated method stub
//	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.filterRow = false;
	}
	

//	@Override
//	public ReturnCode filterKeyValue(Cell c) throws IOException {
//		// TODO Auto-generated method stub
//		byte[] val = CellUtil.cloneValue(c);
//		if(Bytes.compareTo(this.value, val)==0){
//			filterRow = false;
//			return ReturnCode.INCLUDE;
//		}else{
//			ireturn ReturnCode.NEXT_COL;
//		}	
//		//return ReturnCode.INCLUDE_AND_NEXT_COL;
//	}
	
	@Override
	public ReturnCode filterKeyValue(Cell c) throws IOException {
		// TODO Auto-generated method stub
		return ReturnCode.INCLUDE;
	}
	
	public void setFilterRow(boolean filterRow) {
		this.filterRow = filterRow;
	}
	
	@Override
	public byte[] toByteArray(){
		byte[] array = new byte[0];
		array = Bytes.add(array, Bytes.toBytes(this.qualifier.length));
		array = Bytes.add(array, this.qualifier);
		array = Bytes.add(array, Bytes.toBytes(this.value.length));
		array = Bytes.add(array, this.value);
		array = Bytes.add(array, Bytes.toBytes(this.filterRow));
		return array;
	}
	
	public static IdxFilter parseFrom(byte[] bytes) throws DeserializationException{
		IdxFilter filter = null;
		int length = bytes.length;
		
		byte[] qualLength = Bytes.copy(bytes, 0, 4);
		int qualLen = Bytes.toInt(qualLength);
		
		byte[] qualifier = Bytes.copy(bytes, 4, qualLen);
		
		byte[] valLeng = Bytes.copy(bytes, 4+qualLen, 4);
		int valLen = Bytes.toInt(valLeng);
		
		byte[] val = Bytes.copy(bytes, 8+qualLen, valLen);
		
		byte[] fRow = Bytes.copy(bytes, 8+qualLen+valLen, 1);
		boolean filterRow = Bytes.toBoolean(fRow);
		filter = new IdxFilter(qualifier, val, filterRow);
		
		return filter;
	}
	
//	@Override
//	public byte[] toByteArray() throws IOException {
//		// TODO Auto-generated method stub
//		byte[] array = new byte[0];
//		array = Bytes.add(array, Bytes.toBytes(this.value.length));
//		array = Bytes.add(array, this.value);
//		array = Bytes.add(array, Bytes.toBytes(this.filterRow));
//		return array;
//	}
//	
//	public static IdxFilter parseFrom(byte[] bytes) throws DeserializationException{
//		IdxFilter filter = null;
//		int length = bytes.length;
//		
//		byte[] valLeng = Bytes.copy(bytes, 0, 4);
//		int valLen = Bytes.toInt(valLeng);
//		
//		byte[] val = Bytes.copy(bytes, 4, valLen);
//		
//		byte[] fRow = Bytes.copy(bytes, valLen, 1);
//		boolean filterRow = Bytes.toBoolean(fRow);
//		filter = new IdxFilter(val, filterRow);
//		
//		return filter;
//	}
	
}
