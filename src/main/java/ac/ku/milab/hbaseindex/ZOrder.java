package ac.ku.milab.hbaseindex;

public class ZOrder {
	
	public static int Encode(double lat, double lon){
		
		double latitude = lat + 90;
		double longitude = lon + 180;
		
		int latInt = (int) latitude;
		int lonInt = (int) longitude;
		
		int res = 0;
		int a = 1;
		
		for(int i=0;i<18;i++){
			if(i%2==0){
				res = res + (a & latInt);
				lonInt = lonInt << 1;
			}else{
				res = res + (a & lonInt);
				latInt = latInt << 1;
			}
			a = a << 1;
			
		}
		
		return res;
	}
	
	public static int decodeLat(int code){
		int a = 1;
		
		int res = 0;
		for(int i=0;i<9;i++){
			res = res + (a & code);
			code = code >> 1;
			a = a << 1;
		}
		return res - 90;
	}
	
	public static int decodeLon(int code){
		int a = 1;
		
		int res = 0;
		code = code >> 1;
		for(int i=0;i<9;i++){
			res = res + (a & code);
			code = code >> 1;
			a = a << 1;
		}
		return res - 180;
	}
	
	public static void printString(int code){
		System.out.println(Integer.toBinaryString(code));
	}
}
