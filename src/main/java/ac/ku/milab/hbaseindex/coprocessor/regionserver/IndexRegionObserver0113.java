package ac.ku.milab.hbaseindex.coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import ac.ku.milab.hbaseindex.IdxFilter;
import ac.ku.milab.hbaseindex.IdxManager;
import ac.ku.milab.hbaseindex.util.CarNumCovertor;
import ac.ku.milab.hbaseindex.util.TableUtils;

public class IndexRegionObserver0113 extends BaseRegionObserver {
//
//	private static final Log LOG = LogFactory.getLog(IndexRegionObserver0113.class.getName());
//
//	// private IdxManager indexManager = IdxManager.getInstance();
//	private static RTree<byte[], Geometry> regionRTree = RTree.star().create();
//	private static RoaringBitmap bitmap = new RoaringBitmap();
//	
//	private static byte[] key = null;
//
//	@Override
//	public void stop(CoprocessorEnvironment e) throws IOException {
//		// nothing to do here
//	}
//
//	@Override
//	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<Cell> results)
//			throws IOException {
//		// TODO Auto-generated method stub
//
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//
//		LOG.info("preGetOp START : " + tableName);
//
//		// if table is not user table, it is not performed
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//			// Observable<Entry<byte[], Geometry>> entries =
//			// regionRTree.entries();
//			Observable<Entry<byte[], Geometry>> entries = regionRTree
//					.search(RTreeRectangle.create(129.6f, 110.0f, 130.1f, 111.2f));
//			List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
//			LOG.info("number of entry" + list.size());
//			for (Entry<byte[], Geometry> entry : list) {
//				RTreePoint rp = (RTreePoint) entry.geometry();
//				LOG.info("Rtree show - " + rp.getLat() + "," + rp.getLon());
//				LOG.info("Rtree rowkey - " + entry.value());
//				Get rGet = new Get(entry.value());
//				// Result res = ctx.getEnvironment().getRegion().get(rGet);
//				// List<Cell> tmp = res.getColumnCells(Bytes.toBytes("cf1"),
//				// Bytes.toBytes("car_num"));
//				// for(Cell c : tmp){
//				// results.add(c);
//				// }
//			}
//
//			LOG.info("bitmap :" + bitmap);
//			ctx.bypass();
//
//		}
//
//	}
//
//	@Override
//	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
//			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
////
////		// get table's information
////		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
////		String tableName = tName.getNameAsString();
////		//
////		LOG.info("preStoreScannerOpen START : " + tableName);
////		//
////		// // if table is not user table, it is not performed
////		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
////		if (isUserTable) {
////			Filter fil = scan.getFilter();
////			if(fil!=null && fil instanceof SingleColumnValueFilter){
////				//Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
////				Observable<Entry<byte[], Geometry>> entries = regionRTree
////						.search(RTreeRectangle.create(129.4f, 110.5f, 129.9f, 110.9f));
////				List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
////				LOG.info("number of entry" + list.size());
////				List<Filter> filters = new ArrayList<Filter>();
////				Scan sc = new Scan();
////				for (Entry<byte[], Geometry> entry : list) {
////					RTreePoint rp = (RTreePoint) entry.geometry();
////					LOG.info("Rtree show - " + rp.getLat() + "," + rp.getLon());
////
////					Filter f = new RowFilter(CompareOp.EQUAL, new BinaryComparator(entry.value()));
////					filters.add(f);
////				}
////				FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
////				sc.setFilter(filterList);
////				
////				Store newStore = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
////				Map<byte[], NavigableSet<byte[]>> map = sc.getFamilyMap();
////				NavigableSet<byte[]> cols = map.get(Bytes.toBytes("cf1"));
////				ScanInfo scanInfo = newStore.getScanInfo();
////				long ttl = scanInfo.getTtl();
////				scanInfo = new ScanInfo(scanInfo.getConfiguration(),newStore.getFamily(), ttl,
////					scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
////				
////				ctx.complete();
////				return new StoreScanner(newStore, scanInfo, sc, cols,
////						((HStore)newStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
////			}else{
////				Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
////				List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
////				LOG.info("number of entry" + list.size());
////				return s;
////			}
////			
////		}
//
//		// LOG.info("preStoreScannerOpen END : " + tableName);
//
//		// TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		// String tableName = tName.getNameAsString();
//		//
//		// LOG.info("preStoreScannerOpen START : " + tableName);
//		//
//		// boolean isUserTable =
//		// TableUtils.isUserTable(Bytes.toBytes(tableName));
//		// if (isUserTable) {
//		// Filter f = scan.getFilter();
//		// boolean isIndexFilter = (f instanceof IdxFilter);
//		// //boolean isValueFilter = true;
//		//
//		// if (f != null && isIndexFilter) {
//		// String idxTableName = TableUtils.getIndexTableName(tableName);
//		// TableName idxTName = TableName.valueOf(idxTableName);
//		//
//		// List<Region> idxRegions =
//		// ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//		// Region idxRegion = idxRegions.get(0);
//		//
//		// //LOG.info("filter string : " + f.toString());
//		// //Filter indFilter;
//		// //Filter indFilter = new RowFilter(CompareOp.EQUAL, new
//		// BinaryComparator(Bytes.toBytes("idx1v1row1")));
//		//
//		// //LOG.info("preStoreScannerOpen User table : " + tableName + " & " +
//		// idxTableName);
//		//
//		// Scan indScan = new Scan();
//		// indScan.setStartRow(Bytes.toBytes("idx1v1"));
//		// indScan.setStopRow(Bytes.toBytes("idx1v2"));
//		// //indScan.setFilter(indFilter);
//		// Map<byte[], NavigableSet<byte[]>> map = indScan.getFamilyMap();
//		// NavigableSet<byte[]> indCols = map.get(Bytes.toBytes("IND"));
//		// Store indStore = idxRegion.getStore(Bytes.toBytes("IND"));
//		// ScanInfo scanInfo = null;
//		// scanInfo = indStore.getScanInfo();
//		// long ttl = scanInfo.getTtl();
//		//
//		// //LOG.info("filter string : " + indScan.getFilter().toString());
//		//
//		// scanInfo = new ScanInfo(scanInfo.getConfiguration(),
//		// indStore.getFamily(), ttl,
//		// scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
//		// LOG.info("well done");
//		// ctx.complete();
//		// return new StoreScanner(indStore, scanInfo, indScan, indCols,
//		// ((HStore)
//		// indStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
//		// }
//		// }
//		return s;
//	}
//
//	@Override
//	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
//		// TODO Auto-generated method stub
//
//		LOG.info("PreOpen : " + ctx.getEnvironment().getRegionInfo().getTable().getNameAsString());
//		super.preOpen(ctx);
//	}
//
//	// before put implements, call this function
//	@Override
//	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
//			throws IOException {
//
//		// get table's information
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//
//		LOG.info("PrePut START : " + tableName);
//
//		// if table is not user table, it is not performed
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//			String idxTableName = TableUtils.getIndexTableName(tableName);
//			// TableName idxTName = TableName.valueOf(idxTableName);
//
//			Map<byte[], List<Cell>> map = put.getFamilyCellMap();
//			List<Cell> list = map.get(Bytes.toBytes("cf1"));
//
//			// boolean isNum = false;
//			// boolean isLat = false;
//			// boolean isLon = false;
//
//			byte[] carNum = null;
//			byte[] lat = null;
//			byte[] lon = null;
//
//			Cell numCell = list.get(0);
//			carNum = CellUtil.cloneValue(numCell);
//
//			Cell latCell = list.get(1);
//			lat = CellUtil.cloneValue(latCell);
//
//			Cell lonCell = list.get(2);
//			lon = CellUtil.cloneValue(lonCell);
//
//			// byte[] kor = Bytes.copy(carNum, 2, 3);
//
//			String sCarNum = Bytes.toString(carNum);
//			double dLat = Bytes.toDouble(lat);
//			double dLon = Bytes.toDouble(lon);
//
//			// kor = CarNumCovertor.k2i(kor);
//			// byte[] tCarNum = Bytes.add(Bytes.copy(carNum,0, 2),
//			// kor,Bytes.copy(carNum,5, 4));
//			byte[] tCarNum = CarNumCovertor.convert(carNum);
//			int indexNum = CarNumCovertor.toNum(tCarNum);
//			bitmap.add(indexNum);
//
//			// for (Cell c : list) {
//			// byte[] qual = CellUtil.cloneQualifier(c);
//			// if(isNum == false &&)
//			// if (isLat == false && Bytes.equals(qual, Bytes.toBytes("lat"))) {
//			// lat = Bytes.toDouble(CellUtil.cloneValue(c));
//			// isLat = true;
//			// } else if (isLon == false && Bytes.equals(qual,
//			// Bytes.toBytes("lon"))) {
//			// lon = Bytes.toDouble(CellUtil.cloneValue(c));
//			// isLon = true;
//			// } else if(){
//			//
//			// }
//			// if (isLat && isLon) {
//			// break;
//			// }
//			// }
//			//
//			// if (!isLat || !isLon) {
//			// LOG.error("not enough arguments");
//			// return;
//			// }
//
//			byte[] rowKey = put.getRow();
//			RTreePoint rp = RTreePoint.create((float) dLat, (float) dLon);
//			regionRTree = regionRTree.add(rowKey, rp);
//			//LOG.info("Rtree add : " + "num-" + Bytes.toString(rowKey) + "long-" + Bytes.toString(tCarNum) + "lat-" + dLat + " lon-"
//			//		+ dLon);
//		}
//		//
//		// // get table's information
//		// TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		// String tableName = tName.getNameAsString();
//		//
//		// LOG.info("PrePut START : " + tableName);
//		//
//		// // if table is not user table, it is not performed
//		// boolean isUserTable =
//		// TableUtils.isUserTable(Bytes.toBytes(tableName));
//		// if (isUserTable) {
//		// String idxTableName = TableUtils.getIndexTableName(tableName);
//		// TableName idxTName = TableName.valueOf(idxTableName);
//		//
//		// // get index column
//		// List<IdxColumnQualifier> idxColumns =
//		// indexManager.getIndexOfTable(tableName);
//		//// for (IdxColumnQualifier cq : idxColumns) {
//		//// LOG.info("index column : " + cq.getQualifierName());
//		//// }
//		//
//		// // get region
//		// HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
//		// Region region = ctx.getEnvironment().getRegion();at
//		//
//		// // get information of put
//		// Map<byte[], List<Cell>> map = put.getFamibyte[][] nums = new
//		// byte[3][38];lyCellMap();
//		// List<Cell> list = map.get(Bytes.toBytes("cf1"));
//		//
//		// /*
//		// * index table rowkey = region start key + "idx" + all(qualifier
//		// * number + value)
//		// */
//		//
//		// // get region start keys
//		// String startKey = Bytes.toString(hRegionInfo.getStartKey());
//		// String rowKey = startKey + "idx";
//		//
//		// // get column value, id,at
//		// for (Cell c : list) {
//		// String qual = Bytes.toString(CellUtil.cloneQualifier(c));
//		// qual = qual.substring(1);
//		// qual += Bytes.toString(CellUtil.cloneValue(c));
//		// rowKey += qual;
//		// }
//		// rowKey += Bytes.toString(put.getRow());
//		// //LOG.info("Row Key is " + rowKey);
//		//
//		// // make put for index table
//		// Put idxPut = new Put(Bytes.toBytes(rowKey));
//		// idxPut.addColumn(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER,
//		// IdxConstants.IDX_VALUE);
//		//
//		// // index table and put
//		// List<Region> idxRegions =
//		// ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//		// Region idxRegion = idxRegions.get(0);
//		// idxRegion.put(idxPut);
//		// }
//
//		// LOG.info("PrePut END : " + tableName);
//	}
//	
//	
//	
//	@Override
//	public void postPut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
//			throws IOException {
//		// TODO Auto-generated method stub
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//		//
//		LOG.info("preScannerOpen START1 : " + tableName);
//		//
//		// // if table is not user table, it is not performed
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//			Scan sc = new Scan();
//			Store newStore = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
//			Collection<StoreFile> files = newStore.getStorefiles();
//			Iterator<StoreFile> iter = files.iterator();
//			while(iter.hasNext()){
//				StoreFile file = iter.next();
//				Reader r = file.createReader();
//				HFile.Reader hfileReader = r.getHFileReader();
//				HFileScanner scanner = hfileReader.getScanner(false, false);
//				//Cell c = 
//				//scanner.seekTo()
//				//StoreFileScanner storescanner = r.getStoreFileScanner(false, false);
//				
//				//HFile.Reader r1 = r.getHFileReader();
//				//HFileScanner scanner = r1.getScanner(false, false);
//				//Cell c = scanner.getKeyValue();
//				scanner.seekTo();
//				key = scanner.getKey().array();
//				LOG.info("seek key"+key);
//			}
//		}
//	}
//	
//	@Override
//	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
//			throws IOException {
//		// TODO Auto-generated method stub
//		// get table's information
//				TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//				String tableName = tName.getNameAsString();
//				//
//				LOG.info("preScannerOpen START1 : " + tableName);
//				//
//				// // if table is not user table, it is not performed
//				boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//				if (isUserTable) {
//					Filter fil = scan.getFilter();
//					if(fil!=null && fil instanceof SingleColumnValueFilter){
//						Observable<Entry<byte[], Geometry>> entries = regionRTree
//								.search(RTreeRectangle.create(129.4f, 110.5f, 129.9f, 110.9f));
//						List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
//						LOG.info("number of entry" + list.size());
//						list.sort(new Comparator<Entry<byte[], Geometry>>() {
//
//							public int compare(Entry<byte[], Geometry> o1, Entry<byte[], Geometry> o2) {
//								// TODO Auto-generated method stub
//								byte[] key1 = o1.value();
//								byte[] key2 = o2.value();
//								
//								int res = Bytes.compareTo(key1, key2);
//								return res;
//							}
//						});
//						
//						//List<KeyValueScanner> scannerList = new ArrayList<KeyValueScanner>();
//						
//						Scan sc = new Scan();
//						Store newStore = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
//						DefaultMemStore memStore = new DefaultMemStore();
//						//memStore.
//						Collection<StoreFile> files = newStore.getStorefiles();
//						Iterator<StoreFile> iter = files.iterator();
//						int cnt=0;
//						while(iter.hasNext()){
//							StoreFile file = iter.next();
//							Reader r = file.createReader();
//							HFile.Reader hfileReader = r.getHFileReader();
//							HFileScanner scanner = hfileReader.getScanner(false, false);
//							byte[] carNum = Bytes.toBytes("11嫄�1111");
//							byte[] time = Bytes.toBytes(14568879l);
//							byte[] rowKey = Bytes.add(carNum, time);
//							Cell c = CellUtil.createCell(rowKey, Bytes.toBytes("cf1"), Bytes.toBytes("car_num"));
//							
//							int test = scanner.seekTo(c);
//							//scanner.seekTo(key);
//							LOG.info("seek search"+test);
//							//Cell c = 
//							//scanner.seekTo()
//							//StoreFileScanner storescanner = r.getStoreFileScanner(false, false);
//							
//							//HFile.Reader r1 = r.getHFileReader();
//							//HFileScanner scanner = r1.getScanner(false, false);
//							//Cell c = scanner.getKeyValue();
////							LOG.info("Storefile firstkey : "+r.getHFileReader().getFirstKey().length);
////							LOG.info("Storefile lastkey : "+r.getHFileReader().getLastKey().length);
////							cnt++;
////							if(cnt==10000){
////								break;
////							}
//						}
////						Map<byte[], NavigableSet<byte[]>> map = sc.getFamilyMap();
////						NavigableSet<byte[]> cols = map.get(Bytes.toBytes("cf1"));
////						ScanInfo scanInfo = newStore.getScanInfo();
////						long ttl = scanInfo.getTtl();
////						
////						scanInfo = new ScanInfo(scanInfo.getConfiguration(),newStore.getFamily(), ttl,
////							scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
////						
////						for (Entry<byte[], Geometry> entry : list) {
////							RTreePoint rp = (RTreePoint) entry.geometry();
////							//LOG.info("Rtree show - " + rp.getLat() + "," + rp.getLon());
////							StoreScanner storeScanner = new StoreScanner(newStore, scanInfo, sc, cols, ((HStore)newStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
////							scannerList.add(storeScanner);
////						}
////						
////
////						ctx.bypass();
////						ctx.complete();
////						return ctx.getEnvironment().getRegion().getScanner(sc, scannerList);
//					}else{
//						Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
//						List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
//						LOG.info("number of entry" + list.size());
//						return s;
//					}
////					
////					Filter fil = scan.getFilter();
////					if(fil!=null && fil instanceof IdxFilter){
////						//Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
////						Observable<Entry<byte[], Geometry>> entries = regionRTree
////								.search(RTreeRectangle.create(129.4f, 110.5f, 129.9f, 110.9f));
////						List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
////						LOG.info("number of entry" + list.size());
////						List<Filter> filters = new ArrayList<Filter>();
////						Scan sc = new Scan();
////						for (Entry<byte[], Geometry> entry : list) {
////							RTreePoint rp = (RTreePoint) entry.geometry();
////							LOG.info("Rtree show - " + rp.getLat() + "," + rp.getLon());
////
////							Filter f = new RowFilter(CompareOp.EQUAL, new BinaryComparator(entry.value()));
////							filters.add(f);
////						}
////						FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
////						sc.setFilter(filterList);
////						
////						Store newStore = ctx.getEnvironment().getRegion().getStore(Bytes.toBytes("cf1"));
////						Map<byte[], NavigableSet<byte[]>> map = sc.getFamilyMap();
////						NavigableSet<byte[]> cols = map.get(Bytes.toBytes("cf1"));
////						ScanInfo scanInfo = newStore.getScanInfo();
////						long ttl = scanInfo.getTtl();
////						scanInfo = new ScanInfo(scanInfo.getConfiguration(),newStore.getFamily(), ttl,
////							scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
////						ctx.bypass();
////						ctx.complete();
////						
////						return ctx.getEnvironment().getRegion().getScanner(sc);
////					}else{
////						Observable<Entry<byte[], Geometry>> entries = regionRTree.entries();
////						List<Entry<byte[], Geometry>> list = entries.toList().toBlocking().single();
////						LOG.info("number of entry" + list.size());
////						return s;
////					}
////					
//				}return s;
//	}

}
