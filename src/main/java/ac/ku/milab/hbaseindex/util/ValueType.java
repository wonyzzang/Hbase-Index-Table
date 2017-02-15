package ac.ku.milab.hbaseindex.util;

public enum ValueType {
	String, Int, Float, Long, Double, Short, Byte, Char;

	public int convert2Int() {
		return this.ordinal();
	}
	
	public static ValueType convert2Enum(int i){
		switch(i){
		case 0: return String;
		case 1: return Int;
		case 2: return Float;
		case 3: return Long;
		case 4: return Double;
		case 5: return Short;
		case 6: return Byte;
		case 7: return Char;
		default : return String;
		}
	}
}
