package ac.ku.milab.hbaseindex.coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import ac.ku.milab.hbaseindex.IdxFilter;
import ac.ku.milab.hbaseindex.IdxManager;
import ac.ku.milab.hbaseindex.SingleColumnFilterWrapper;
import ac.ku.milab.hbaseindex.util.CarNumCovertor;
import ac.ku.milab.hbaseindex.util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	// private IdxManager indexManager = IdxManager.getInstance();
	// private static RTree<byte[], Geometry> regionRTree =
	// RTree.star().create();
	// private static RoaringBitmap bitmap = new RoaringBitmap();

	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");

	// private static List<byte[]> memStoreList = new ArrayList<byte[]>();
	// private static ArrayList<StoreFileWrapper> wrapperList = new
	// ArrayList<StoreFileWrapper>();

	// private static byte[] key = null;
	private static int count = 0;
	private static int cnt = 0;

	private static int wrapperNumber = 0;

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		// nothing to do here
	}

	@Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		//
		LOG.info("preScannerOpen START1 : " + tableName);
		//
		// // if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			
		} else {
			
			return s;
		}
		return s;
	}
	// @Override
	// public InternalScanner
	// preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
	// InternalScanner scanner) throws IOException {
	// // TODO Auto-generated method stub
	//
	// ArrayList<String> array = new ArrayList<String>();
	// long startTime = 0;
	// long endTime = 0;
	// double x1;
	// double y1;
	// double x2;
	// double y2;
	//
	// return super.preFlush(e, store, scanner);
	// }

	// @Override
	// public InternalScanner
	// preFlush(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
	// InternalScanner scanner) throws IOException {
	// // TODO Auto-generated method stub
	//
	// TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
	// String tableName = tName.getNameAsString();
	//
	// boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
	// if (isUserTable) {
	//
	// List<Cell> cells = new ArrayList<Cell>();
	// boolean haveNext = false;
	// byte[] preRow = Bytes.toBytes("0");
	// byte[] curRow;
	// while (true) {
	// haveNext = scanner.next(cells);
	// Cell c = cells.get(0);
	// curRow = Bytes.copy(CellUtil.cloneRow(c), 0, 9);
	// if (Bytes.equals(curRow, preRow)) {
	// } else {
	// break;
	// }
	//
	// if (haveNext == false) {
	// break;
	// }
	// }
	// if (haveNext == true) {
	// return scanner;
	// } else {
	// return super.preFlush(ctx, store, scanner);
	// }
	// }
	// return super.preFlush(ctx, store, scanner);
	// }

	@Override
	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
			List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s,
			CompactionRequest request) throws IOException {
		// TODO Auto-generated method stub
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			for (KeyValueScanner scanner : scanners) {
				if (scanner instanceof StoreFileScanner) {
					StoreFileScanner SFScanner = (StoreFileScanner) scanner;
					byte[] firstKey = CellUtil.getCellKeySerializedAsKeyValueKey(SFScanner.next());
					LOG.info("preCompact - first key is " + firstKey);
				}

			}
		}
		return super.preCompactScannerOpen(ctx, store, scanners, scanType, earliestPutTs, s, request);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store, StoreFile resultFile)
			throws IOException {
		// TODO Auto-generated method stub

		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			LOG.info("PostCompact start");

			ArrayList<String> array = new ArrayList<String>();
			long startTime = Long.MAX_VALUE;
			long endTime = Long.MIN_VALUE;
			double lat1 = 90.0;
			double lon1 = 180;
			double lat2 = 0;
			double lon2 = 0;

			byte[] firstKey = resultFile.getFirstKey();
			// key = firstKey;
			LOG.info("First Key is " + firstKey);
			HFileScanner scanner = resultFile.getReader().getHFileReader().getScanner(false, false);
			if (scanner.seekTo()) {
				while (true) {
					Cell c = scanner.getKeyValue();
					byte[] qualifier = CellUtil.cloneQualifier(c);
					if (Bytes.equals(qualifier, Bytes.toBytes("car_num"))) {
						String number = Bytes.toString(CellUtil.cloneValue(c));
						// LOG.info("-c-number is " + number);
						boolean isExist = false;
						for (int i = 0; i < array.size(); i++) {
							if (number.equals(array.get(i))) {
								isExist = true;
								break;
							}
						}
						if (isExist == false) {
							array.add(number);
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("time"))) {
						long time = Bytes.toLong(CellUtil.cloneValue(c));
						if (time > endTime) {
							endTime = time;
						}
						if (time < startTime) {
							startTime = time;
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("lat"))) {
						double lat = Bytes.toDouble(CellUtil.cloneValue(c));
						if (lat > lat2) {
							lat2 = lat;
						}
						if (lat < lat1) {
							lat1 = lat;
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("lon"))) {
						double lon = Bytes.toDouble(CellUtil.cloneValue(c));
						if (lon > lon2) {
							lon2 = lon;
						}
						if (lon < lon1) {
							lon1 = lon;
						}
					}
					if (!scanner.next()) {
						
						break;
					}
					cnt++;
				}

			}
		}
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store, StoreFile resultFile)
			throws IOException {
		// TODO Auto-generated method stub

		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		LOG.info("PostFlush start " + tableName);

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {

			ArrayList<String> array = new ArrayList<String>();
			long startTime = Long.MAX_VALUE;
			long endTime = Long.MIN_VALUE;
			double lat1 = 90.0;
			double lon1 = 180;
			double lat2 = 0;
			double lon2 = 0;

			byte[] firstKey = resultFile.getFirstKey();
			// key = firstKey;
			LOG.info("First Key is " + firstKey);
			HFileScanner scanner = resultFile.getReader().getHFileReader().getScanner(false, false);
			if (scanner.seekTo()) {
				while (true) {
					Cell c = scanner.getKeyValue();
					byte[] qualifier = CellUtil.cloneQualifier(c);
					if (Bytes.equals(qualifier, Bytes.toBytes("car_num"))) {
						String number = Bytes.toString(CellUtil.cloneValue(c));
						// LOG.info("-f-number is " + number);
						boolean isExist = false;
						for (int i = 0; i < array.size(); i++) {
							if (number.equals(array.get(i))) {
								isExist = true;
								break;
							}
						}
						if (isExist == false) {
							array.add(number);
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("time"))) {
						long time = Bytes.toLong(CellUtil.cloneValue(c));
						if (time > endTime) {
							endTime = time;
						}
						if (time < startTime) {
							startTime = time;
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("lat"))) {
						double lat = Bytes.toDouble(CellUtil.cloneValue(c));
						if (lat > lat2) {
							lat2 = lat;
						}
						if (lat < lat1) {
							lat1 = lat;
						}
					} else if (Bytes.equals(qualifier, Bytes.toBytes("lon"))) {
						double lon = Bytes.toDouble(CellUtil.cloneValue(c));
						if (lon > lon2) {
							lon2 = lon;
						}
						if (lon < lon1) {
							lon1 = lon;
						}
					}
					if (!scanner.next()) {
						LOG.info("CNT is " + cnt);
						
						break;
					}
					cnt++;
				}

			}
		}
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		// TODO Auto-generated method stub

		LOG.info("PreOpen : " + ctx.getEnvironment().getRegionInfo().getTable().getNameAsString());
		super.preOpen(ctx);
	}

	// before put implements, call this function
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {

		// get table's information
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		// LOG.info("PrePut START : " + tableName);

		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			Map<byte[], List<Cell>> map = put.getFamilyCellMap();
			List<Cell> list = map.get(COLUMN_FAMILY);

			byte[] carNum = null;
			byte[] time = null;
			byte[] lat = null;
			byte[] lon = null;

			Cell numCell = list.get(0);
			carNum = CellUtil.cloneValue(numCell);

			Cell timeCell = list.get(1);
			time = CellUtil.cloneValue(timeCell);

			Cell latCell = list.get(2);
			lat = CellUtil.cloneValue(latCell);

			Cell lonCell = list.get(3);
			lon = CellUtil.cloneValue(lonCell);

			String sCarNum = Bytes.toString(carNum);
			double dLat = Bytes.toDouble(lat);
			double dLon = Bytes.toDouble(lon);

			// byte[] tCarNum = CarNumCovertor.convert(carNum);

			byte[] rowKey = put.getRow();
			// RTreePoint rp = RTreePoint.create((float) dLat, (float) dLon);
			// regionRTree = regionRTree.add(rowKey, rp);
			// LOG.info("Rtree add : " + "num-" + Bytes.toString(rowKey) +
			// "long-" + Bytes.toString(tCarNum) + "lat-" + dLat + " lon-"
			// + dLon);
			// memStoreList.add(rowKey);
			count++;
			if (count == 100) {
				ctx.getEnvironment().getRegion().flush(true);
				count = 0;
			}

		}
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, Region l, Region r) throws IOException {
		// TODO Auto-generated method stub
		super.postSplit(e, l, r);
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		// Store s = ctx.getEnvironment().getRegion().getStore(COLUMN_FAMILY);
	}
//
//	@Override
//	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
//			throws IOException {
//		// TODO Auto-generated method stub
//		
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//		
//		LOG.info("preScannerOpen START1 : " + tableName);
//		
//		// // if table is not user table, it is not performed
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//			Filter fil = scan.getFilter();
//			if (fil != null && fil instanceof SingleColumnFilterWrapper) {
//				Store store = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
//				LOG.info("preScannerOpen Filter set");
//				DefaultMemStore memstore = new DefaultMemStore();
//				List<KeyValueScanner> list = memstore.getScanners(0);
//				SingleColumnFilterWrapper SCVFilter = (SingleColumnFilterWrapper) fil;
//				byte[] targetValue = SCVFilter.getTargetValue();
//				ArrayList<StoreFile> storeFileList= (ArrayList) store.getStorefiles();
//				double lat = Bytes.toLong(targetValue);
//				ArrayList<StoreFileWrapper> storeFileWrapList = (ArrayList) wrapperMap.values();
//				for(StoreFileWrapper wrapper : storeFileWrapList){
//					if(wrapper.x1 >= lat){
//						byte[] key = wrapper.startKey;
//						for(StoreFile file : storeFileList){
//							if(Bytes.equals(key, file.getFirstKey())){
//								list.add(file.createReader().getStoreFileScanner(true, true));
//								LOG.info("preStoreScanner Condition Complete");
//								break;
//							}
//						}
//					}
//				}
//				Scan newScan = new Scan();
//				ScanInfo info = store.getScanInfo();
//				
//				StoreScanner resultScanner = new StoreScanner(store, info, newScan, list, ScanType.USER_SCAN, 0, 0);
//				return resultScanner;
//				
//			}
//		return super.preScannerOpen(ctx, scan, s);
//	}

//	@Override
//	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
//			throws IOException {
//		// TODO Auto-generated method stub
//		// get table's information
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//		//
//		LOG.info("preScannerOpen START1 : " + tableName);
//		//
//		// // if table is not user table, it is not performed
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//			Filter fil = scan.getFilter();
//			if (fil != null && fil instanceof SingleColumnValueFilter) {
////				Observable<Entry<byte[], Geometry>> entries = regionRTree
////						.search(RTreeRectangle.create(129.4f, 110.5f, 129.9f, 110.9f));
////				List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
//				// LOG.info("number of entry" + list.size());
//				// list.sort(new Comparator<Entry<byte[], Geometry>>() {
//				//
//				// public int compare(Entry<byte[], Geometry> o1, Entry<byte[],
//				// Geometry> o2) {
//				// // TODO Auto-generated method stub
//				// byte[] key1 = o1.value();
//				// byte[] key2 = o2.value();
//				//
//				// int res = Bytes.compareTo(key1, key2);
//				// return res;
//				// }
//				// });
//
//				// for(Entry<byte[], Geometry> ent : list){
//				// byte[] val = ent.value();
//				//
//				// for(byte[] rowkey : memStoreList){
//				// if(Bytes.equals(val, rowkey)){
//				// DefaultMemStore memStore = new DefaultMemStore();
//				// KeyValueScanner scanner = memStore.snapshot().getScanner();
//				// //scanner.
//				// }
//				// }
//				// }
//
//				// for(Entry<byte[], Geometry> ent : list){
//				// byte[] val = ent.value();
//				// Scan sc = new Scan();
//				// sc.setStartRow(val);
//				// sc.setStopRow(Bytes.incrementBytes(val, 1l));
//				// }
//
//				// List<KeyValueScanner> scannerList = new
//				// ArrayList<KeyValueScanner>();
//
////				Scan sc = new Scan();
////				Store newStore = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
////				Map<byte[], NavigableSet<byte[]>> map = sc.getFamilyMap();
////				NavigableSet<byte[]> cols = map.get(Bytes.toBytes("cf1"));
////				ScanInfo scanInfo = newStore.getScanInfo();
////				long ttl = scanInfo.getTtl();
////				scanInfo = new ScanInfo(scanInfo.getConfiguration(), newStore.getFamily(), ttl,
////						scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
////				ctx.complete();
////
////				List<KeyValueScanner> scannerList = new ArrayList<KeyValueScanner>();
////				for (Entry<byte[], Geometry> ent : list) {
////					byte[] val = ent.value();
////					sc = new Scan();
////					sc.setStartRow(val);
////					sc.setStopRow(Bytes.incrementBytes(Bytes.copy(val, 9, 8), 1l));
////					KeyValueScanner scanner = new StoreScanner(newStore, scanInfo, sc, cols,
////							((HStore) newStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
////					scannerList.add(scanner);
////				}
////				sc = new Scan();
////				sc.setStartRow(Bytes.toBytes("22�삤2222"));
////				LOG.info("number of entry" + list.size());
//
////				return ctx.getEnvironment().getRegion().getScanner(sc, scannerList);
//			}
//
//			// Iterator<StoreFile> iter = files.iterator();
//			// while(iter.hasNext()){
//			// StoreFile file = iter.next();
//			// if(Bytes.equals(key, file.getFirstKey())){
//			// Reader r = file.createReader();
//			// HFile.Reader hfileReader = r.getHFileReader();
//			//
//			// HFileScanner scanner = hfileReader.getScanner(true, true);
//			// LOG.info("Key is Correct");
//			// }
//
//			// Reader r = file.createReader();
//			// HFile.Reader hfileReader = r.getHFileReader();
//			//
//			// HFileScanner scanner = hfileReader.getScanner(true, true);
//			// Cell c =
//			// scanner.seekTo()
//			// StoreFileScanner storescanner = r.getStoreFileScanner(false,
//			// false);
//
//			// HFile.Reader r1 = r.getHFileReader();
//			// HFileScanner scanner = r1.getScanner(false, false);
//			// Cell c = scanner.getKeyValue();
//			// scanner.seekTo();
//			// key = scanner.getKey().array();
//			// LOG.info("seek key"+key);
//			// LOG.info("seek key"+hfileReader.getFirstRowKey());
//			// }
//
//		} else {
//			Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
//			List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
//			LOG.info("number of entry" + list.size());
//			return s;
//		}
//
//		return s;
//	}

}
