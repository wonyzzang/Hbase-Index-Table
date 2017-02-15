package ac.ku.milab.hbaseindex.coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class IndexRegionScanner implements RegionScanner {
	private static final Log LOG = LogFactory.getLog(IndexRegionScanner.class);

	private RegionScanner tableScanner = null;
	private RegionScanner indexScanner = null;
	private Scan scan = null;

	private Cell currentCell = null;
	private int scannerIndex = -1;
	private boolean hasMore = true;
	private boolean isClosed = false;

	private RowFilter filter = null;

	// private SingleColumnSearchFilter filter = null;

	public IndexRegionScanner(RegionScanner table,RegionScanner index, Scan scan) {
		this.tableScanner = table;
		this.indexScanner = table;
		this.scan = scan;

		LOG.info("IndexRegionScanner Open");
	}
	
	public void close() throws IOException {
		tableScanner.close();
		indexScanner.close();
		isClosed = true;
	}

	public long getMvccReadPoint() {
		return indexScanner.getMvccReadPoint();
	}

	public HRegionInfo getRegionInfo() {
		return indexScanner.getRegionInfo();
	}

	public boolean isFilterDone() {
		try {
			return indexScanner.isFilterDone();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean reseek(byte[] row) throws IOException {
		if (!hasMore) {
			return false;
		}
		return indexScanner.reseek(row);
	}

	public Scan getScan() {
		return this.scan;
	}

	public RegionScanner getRegionScanner() {
		return this.indexScanner;
	}

	public boolean isClosed() {
		return this.isClosed;
	}

	// check if more rows exist after this row
	public boolean next(List<Cell> list) throws IOException {
		if (!this.hasMore) {
			return false;
		}

		boolean tmpHasMore = this.indexScanner.next(list);
		if (list != null && list.size() > 0) {
			Cell c = list.get(0);
			this.currentCell = c;
		}

		while (list.size() < 1 && tmpHasMore) {
			tmpHasMore = this.indexScanner.next(list);
			if (list != null && list.size() > 0) {
				Cell c = list.get(0);
				this.currentCell = c;
			}
		}

		this.hasMore = tmpHasMore;
		return tmpHasMore;
	}

	public boolean next(List<Cell> list, ScannerContext ctx) throws IOException {
		return indexScanner.next(list, ctx);
	}

	public int getBatch() {
		return 0;
	}

	public long getMaxResultSize() {
		return 0;
	}

	public boolean nextRaw(List<Cell> list) throws IOException {
		return indexScanner.nextRaw(list);
	}

	public boolean nextRaw(List<Cell> list, ScannerContext ctx) throws IOException {
		return indexScanner.nextRaw(list, ctx);
	}

}
