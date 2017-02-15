package ac.ku.milab.hbaseindex.util;

import org.apache.hadoop.hbase.util.Bytes;

public class CarNumCovertor{
	static final byte[][] carNumKorean = new byte[][]{
		Bytes.toBytes("가"), Bytes.toBytes("거"),Bytes.toBytes("고"), Bytes.toBytes("구"),Bytes.toBytes("나"),
		Bytes.toBytes("너"),  Bytes.toBytes("노"), Bytes.toBytes("누"),Bytes.toBytes("다"),Bytes.toBytes("더"),
		Bytes.toBytes("도"), Bytes.toBytes("두"), Bytes.toBytes("라"), Bytes.toBytes("러"),Bytes.toBytes("로"), 
		Bytes.toBytes("루"),  Bytes.toBytes("마"), Bytes.toBytes("머"),Bytes.toBytes("모"), Bytes.toBytes("무"), 
		Bytes.toBytes("바"), Bytes.toBytes("배"), Bytes.toBytes("버"), Bytes.toBytes("보"), Bytes.toBytes("부"),
		Bytes.toBytes("사"), Bytes.toBytes("서"), Bytes.toBytes("소"), Bytes.toBytes("수"),Bytes.toBytes("아"), 
		Bytes.toBytes("어"), Bytes.toBytes("오"), Bytes.toBytes("우"),Bytes.toBytes("자"),  Bytes.toBytes("저"), 
		Bytes.toBytes("조"), Bytes.toBytes("주"),Bytes.toBytes("하"),Bytes.toBytes("허"),Bytes.toBytes("호") };
		
		static final byte[][] carNumTarget = new byte[][]{
			Bytes.toBytes("00"), Bytes.toBytes("01"), Bytes.toBytes("02"), Bytes.toBytes("03"), Bytes.toBytes("04"), 
			Bytes.toBytes("05"), Bytes.toBytes("06"), Bytes.toBytes("07"), Bytes.toBytes("08"), Bytes.toBytes("09"), 
			Bytes.toBytes("10"), Bytes.toBytes("11"), Bytes.toBytes("12"), Bytes.toBytes("13"), Bytes.toBytes("14"), 
			Bytes.toBytes("15"), Bytes.toBytes("16"), Bytes.toBytes("17"), Bytes.toBytes("18"), Bytes.toBytes("19"),
			Bytes.toBytes("20"), Bytes.toBytes("21"), Bytes.toBytes("22"), Bytes.toBytes("23"), Bytes.toBytes("24"),
			Bytes.toBytes("25"), Bytes.toBytes("26"), Bytes.toBytes("27"), Bytes.toBytes("28"), Bytes.toBytes("29"),
			Bytes.toBytes("30"), Bytes.toBytes("31"), Bytes.toBytes("32"), Bytes.toBytes("33"), Bytes.toBytes("34"),
			Bytes.toBytes("35"), Bytes.toBytes("36"), Bytes.toBytes("37"), Bytes.toBytes("38"), Bytes.toBytes("39") };
		
	static final int length = carNumKorean.length;
	
	public CarNumCovertor(){
		
	}
	
	public static byte[] k2i(byte[] target){
		byte[] res = null;
		for(int i=0; i<length; i++){
			if(Bytes.equals(target, carNumKorean[i])){
				res = carNumTarget[i];
				break;
			}
		}
		return res;
	}
	
	public static byte[] i2k(byte[] target){
		byte[] res = null;
		for(int i=0; i<length; i++){
			if(Bytes.equals(target, carNumTarget[i])){
				res = carNumKorean[i];
				break;
			}
		}
		return res;
	}
	
	public static int toNum(byte[] target){
		String s = Bytes.toString(target);
		int num = Integer.valueOf(s);
		return num;
	}
	
	public static byte[] convert(byte[] ori){
		byte[] kor = Bytes.copy(ori, 2, 3);
		kor = CarNumCovertor.k2i(kor);
		byte[] tCarNum = Bytes.add(Bytes.copy(ori,0, 2), kor,Bytes.copy(ori,5, 4));
		return tCarNum;
	}
}