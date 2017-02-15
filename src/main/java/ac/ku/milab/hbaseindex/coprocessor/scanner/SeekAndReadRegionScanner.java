package ac.ku.milab.hbaseindex.coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class SeekAndReadRegionScanner implements RegionScanner {
	
	private IndexRegionScanner scanner;
	private Scan scan;
	private HRegion region;
	private byte[] startRow;
	
	private boolean isClosed = false;
	
	public SeekAndReadRegionScanner(IndexRegionScanner scanner, Scan scan, HRegion region, byte[] startRow) {
		this.scanner = scanner;
		this.scan = scan;
		this.region = region;
		this.startRow = startRow;
	}
	
	public void close() throws IOException {
		this.scanner.close();
		this.isClosed = true;
	}

	public long getMvccReadPoint() {
		return 0;
	}

	public HRegionInfo getRegionInfo() {
		return this.scanner.getRegionInfo();
	}

	public boolean isFilterDone() {
		return this.scanner.isFilterDone();
	}

	public boolean reseek(byte[] row) throws IOException {
		return this.scanner.reseek(row);
	}

	public boolean next(List<Cell> list) throws IOException {
		return false;
	}

	public boolean next(List<Cell> list, ScannerContext ctx) throws IOException {
		boolean hasNext = false;
		if(this.scanner.isClosed()){
			return hasNext;
		}else{
			hasNext = this.scanner.next(list, ctx);
		}
		
	   return hasNext;
	}

	public int getBatch() {
		return 0;
	}

	public long getMaxResultSize() {
		return 0;
	}

	public boolean nextRaw(List<Cell> list) throws IOException {
		return false;
	}

	public boolean nextRaw(List<Cell> list, ScannerContext ctx) throws IOException {
		return false;
	}

}
