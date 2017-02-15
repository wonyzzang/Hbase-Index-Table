package ac.ku.milab.hbaseindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import ac.ku.milab.hbaseindex.util.ValueType;

/* This class is for index column qualifier */
public class IdxColumnQualifier implements WritableComparable<IdxColumnQualifier>{
	
	private String qualifierName;  // qualifier name
	private ValueType valueType;   // value's type
	
	public IdxColumnQualifier(){
		this.valueType = ValueType.String;
	}
	
	public IdxColumnQualifier(String qualifier, ValueType vt){
		this.qualifierName = qualifier;
		this.valueType = vt;
	}
	
	public void setQualifierName(String name){
		this.qualifierName = name;
	}
	
	/**
	 * @param 
	 * @return index column qualifier's name
	 */
	
	public String getQualifierName(){
		return this.qualifierName;
	}
	
	/**
	 * @param 
	 * @return length of index column qualifier's name
	 */
	
	public int getQulifierLength(){
		return this.qualifierName.length();
	}
	
	public void setValueType(ValueType vt){
		this.valueType = vt;
	}

	
	/**
	 * @param 
	 * @return index column qualifier's value type
	 */
	
	public ValueType getValueType(){
		return this.valueType;
	}

	public void readFields(DataInput in) throws IOException {
		this.qualifierName = in.readLine();
		this.valueType = ValueType.convert2Enum(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, Bytes.toBytes(this.qualifierName));
		out.writeInt(this.valueType.convert2Int());
	}

	public int compareTo(IdxColumnQualifier iq) {
		int result=0;
		if(this.valueType!=iq.valueType){
			return result;
		}
		
		switch(this.valueType){
			case String : result = this.qualifierName.compareTo(iq.getQualifierName());
				break;
			default:
				break;
		}

		return result<0 ? -result : result;
	}
}
