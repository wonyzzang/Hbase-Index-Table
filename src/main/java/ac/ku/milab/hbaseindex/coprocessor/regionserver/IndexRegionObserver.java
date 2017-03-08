package ac.ku.milab.hbaseindex.coprocessor.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.SplitTransactionFactory;
import org.apache.hadoop.hbase.regionserver.SplitTransactionImpl;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;

import ac.ku.milab.hbaseindex.IdxFilter;
import ac.ku.milab.hbaseindex.util.IdxConstants;
import ac.ku.milab.hbaseindex.util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	//private IdxManager indexManager = IdxManager.getInstance();

	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");
	private static int cnt = 0;

//	@Override
//	public void stop(CoprocessorEnvironment e) throws IOException {
//		// nothing to do here
//	}

	@Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		
		LOG.info("preStoreScannerOpen START : " + tableName);
		
		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			Filter f = scan.getFilter();
			boolean isIndexFilter = (f instanceof IdxFilter);
		if(f!=null && isIndexFilter){
			
			String idxTableName = TableUtils.getIndexTableName(tableName);
			TableName idxTName = TableName.valueOf(idxTableName);
			
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			Region idxRegion = idxRegions.get(0);
			
			//LOG.info("filter string : " + f.toString());
			//Filter indFilter;
			//Filter indFilter = new RowFilter(CompareOp.EQUAL, new
			//BinaryComparator(Bytes.toBytes("idx1v1row1")));
			
			//LOG.info("preStoreScannerOpen User table : " + tableName + " & " +
			// idxTableName);
			
			 Scan indScan = new Scan();
			 //indScan.setStartRow(Bytes.toBytes("idx1v1"));
			 //indScan.setStopRow(Bytes.toBytes("idx1v2"));
			 //indScan.setFilter(indFilter);
			 Map<byte[], NavigableSet<byte[]>> map = indScan.getFamilyMap();
			 NavigableSet<byte[]> indCols = map.get(Bytes.toBytes("IND"));
			 Store indStore = idxRegion.getStore(Bytes.toBytes("IND"));
			 ScanInfo scanInfo = null;
			 scanInfo = indStore.getScanInfo();
			 long ttl = scanInfo.getTtl();
			
			 //LOG.info("filter string : " + indScan.getFilter().toString());
			
			 scanInfo = new ScanInfo(scanInfo.getConfiguration(),
			 indStore.getFamily(), ttl,
			 scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
			 LOG.info("well done");
			 ctx.complete();
			 return new StoreScanner(indStore, scanInfo, indScan, indCols, ((HStore)indStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
			 }
		}
		return s;
	}

//	@Override
//	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
//			List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s,
//			CompactionRequest request) throws IOException {
//		// TODO Auto-generated method stub
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//
//		}
//		return super.preCompactScannerOpen(ctx, store, scanners, scanType, earliestPutTs, s, request);
//	}
//
//	@Override
//	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store, StoreFile resultFile)
//			throws IOException {
//		// TODO Auto-generated method stub
//
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//
//		}
//	}
//
//	@Override
//	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store, StoreFile resultFile)
//			throws IOException {
//		// TODO Auto-generated method stub
//
//		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
//		String tableName = tName.getNameAsString();
//
//		LOG.info("PostFlush start " + tableName);
//
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
//		if (isUserTable) {
//
//		}
//	}
	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> ctx, Region l, Region r) throws IOException {
		// TODO Auto-generated method stub
		TableName tableName = ctx.getEnvironment().getRegionInfo().getTable();
		String sTableName = tableName.getNameAsString();
		
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(sTableName));
		
		if(isUserTable){
			RegionServerServices rsService = ctx.getEnvironment().getRegionServerServices();
			ServerName serverName = rsService.getServerName();
			
			byte[] leftRegionKey = l.getRegionInfo().getStartKey();
			byte[] rightRegionKey = r.getRegionInfo().getStartKey();
			LOG.info("left key is" + leftRegionKey);
			LOG.info("right key is" + rightRegionKey);
			
			String sIdxTableName = TableUtils.getIndexTableName(sTableName);
			TableName idxTableName = TableName.valueOf(sIdxTableName);
			
			List<Region> idxRegionList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTableName);
			
//			for(Region idxRegion : idxRegionList){
//				byte[] splitKey = Bytes.add(Bytes.toBytes("22거1234"), Bytes.toBytes(14568879l));
//				
//				r.startRegionOperation();
//				LOG.info("Split Start");
//				String arg = "bin/hbase org.apache.hadoop.hbase.util.RegionSplitter -r -o 2 myTable UniformSplit";
//				String[] args = new String[]{"bin/hbase org.apache.hadoop.hbase.util.RegionSplitter", "-r", "-o", "2", "myTable", "UniformSplit"};
//				try {
//					RegionSplitter.main(args);
//					LOG.info("Split Complete");
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					LOG.info("Split Fail");
//					e.printStackTrace();
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					LOG.info("Split Fail");
//					e.printStackTrace();
//				}
//				
//				r.closeRegionOperation();
			
			for(Region idxRegion : idxRegionList){
				byte[] regionKey = idxRegion.getRegionInfo().getStartKey();
				if(Bytes.contains(regionKey, leftRegionKey)){
					//HexStringSplit splitter = new RegionSplitter.HexStringSplit();
					//splitter.split(leftRegionKey, rightRegionKey);
					SplitTransactionImpl splitter = new SplitTransactionImpl(idxRegion, rightRegionKey);
					if(splitter.prepare()){
						try{
							splitter.execute((Server)rsService, rsService);
							LOG.info("Split Complete");
						}catch(Exception e){
							e.printStackTrace();
						}
					}
					
//					byte[] splitKey = Bytes.add(Bytes.toBytes("22거1234"), Bytes.toBytes(14568879l));
////					HexStringSplit split = new RegionSplitter.HexStringSplit();
////					split.split(2);
//					//r.closeRegionOperation();
//					r.startRegionOperation();
//					LOG.info("Split Start");
//					String arg = "bin/hbase org.apache.hadoop.hbase.util.RegionSplitter -r -o 2 myTable UniformSplit";
//					String[] args = new String[]{"bin/hbase org.apache.hadoop.hbase.util.RegionSplitter", "-r", "-o", "2", "myTable", "UniformSplit"};
//					try {
//						RegionSplitter.main(args);
//						LOG.info("Split Complete");
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						LOG.info("Split Fail");
//						e.printStackTrace();
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						LOG.info("Split Fail");
//						e.printStackTrace();
//					}
//					
//					r.closeRegionOperation();
//					
				}
			}
		}
	}
//	
//	@Override
//	public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
//		// TODO Auto-generated method stub
//		RegionCoprocessorEnvironment env = ctx.getEnvironment();
//		TableName tableName = env.getRegionInfo().getTable();
//		String sTableName = tableName.getNameAsString();
//		
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(sTableName));
//		if(isUserTable){
//			byte[] startKey = env.getRegionInfo().getStartKey();
//			TableName idxTableName = TableName.valueOf(TableUtils.getIndexTableName(sTableName));
//
//			boolean isAlreadySplit = false;
//			List<Region> idxRegionList = env.getRegionServerServices().getOnlineRegions(idxTableName);
//			for(Region idxRegion : idxRegionList){
//				byte[] regionKey = idxRegion.getRegionInfo().getStartKey();
//				if(Bytes.contains(regionKey, startKey)){
//					isAlreadySplit = true;
//					break;
//				}
//			}
//			
//			if(!isAlreadySplit){
//				
//			}
//		}
//		
//	}
	

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
			String idxTableName = TableUtils.getIndexTableName(tableName);
			TableName idxTName = TableName.valueOf(idxTableName);

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

			byte[] indexRowKey = Bytes.add(carNum, time);
			indexRowKey = Bytes.add(indexRowKey, lat, lon);

			String sCarNum = Bytes.toString(carNum);
			long lTime = Bytes.toLong(time);
			double dLat = Bytes.toDouble(lat);
			double dLon = Bytes.toDouble(lon);
			
			//LOG.info("prePut processing - " + sCarNum + ","+lTime+","+dLat+","+dLon);

			// get index column
//			List<IdxColumnQualifier> idxColumns = indexManager.getIndexOfTable(tableName);
//			for (IdxColumnQualifier cq : idxColumns) {
//				LOG.info("index column : " + cq.getQualifierName());
//			}

			// get region
			HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
			Region region = ctx.getEnvironment().getRegion();

			/*
			 * index table rowkey = region start key + "idx" + all(qualifier
			 * number + value)
			 */

			// get region start keys
			byte[] startKey = hRegionInfo.getStartKey();
			indexRowKey = Bytes.add(startKey, indexRowKey);
			//String startKey = Bytes.toString(hRegionInfo.getStartKey());
			//String rowKey = startKey + "idx";

			// get column value, id,at
//			for (Cell c : list) {
//				String qual = Bytes.toString(CellUtil.cloneQualifier(c));
//				qual = qual.substring(1);
//				qual += Bytes.toString(CellUtil.cloneValue(c));
//				rowKey += qual;
//			}
//			rowKey += Bytes.toString(put.getRow());
			// LOG.info("Row Key is " + rowKey);

			// make put for index table
			Put idxPut = new Put(indexRowKey);
			idxPut.addColumn(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, IdxConstants.IDX_VALUE);

			// index table and put
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			int size = idxRegions.size();
			for(int i=0;i<size;i++){
				Region r = idxRegions.get(i);
				byte[] idxStartKey = r.getRegionInfo().getStartKey();
				int compareRes = Bytes.compareTo(indexRowKey, idxStartKey);
				if(compareRes!=-1){
					if(i==size-1||compareRes==0){
						//LOG.info("index OK");
						r.put(idxPut);
					}else{
						Region nextRegion = idxRegions.get(i+1);
						byte[] idxNextKey = nextRegion.getRegionInfo().getStartKey();
						if(Bytes.compareTo(indexRowKey, idxNextKey)==-1){
							//LOG.info("index OK");
							r.put(idxPut);
						}
					}
				}
			}
			//Region idxRegion = idxRegions.get(0);
			//idxRegion.put(idxPut);
			
//			cnt++;
//			if(cnt==100){
//				Region r = ctx.getEnvironment().getRegion();
//				r.startRegionOperation(Operation.SPLIT_REGION);
//				byte[] splitKey = Bytes.add(Bytes.toBytes("22거1234"), Bytes.toBytes(14568879l));
//				SplitTransactionImpl splitter = new SplitTransactionImpl(r, splitKey);
//				if(splitter.prepare()){
//					try{
//						splitter.execute((Server)ctx.getEnvironment().getRegionServerServices(), ctx.getEnvironment().getRegionServerServices());
//						LOG.info("Split Complete");
//					}catch(Exception e){
//						e.printStackTrace();
//						LOG.info("Split Fail");
//					}
//				}
//				r.closeRegionOperation();
//			}
		}
		
		//LOG.info("PrePut END : " + tableName);

	}
	
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		// LOG.info("PrePut START : " + tableName);

		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			cnt++;
			if(cnt==100){
				Region r = ctx.getEnvironment().getRegion();
				//r.startRegionOperation(Operation.SPLIT_REGION);
//				byte[] splitKey = Bytes.add(Bytes.toBytes("22거1234"), Bytes.toBytes(14568879l));
////				HexStringSplit split = new RegionSplitter.HexStringSplit();
////				split.split(2);
//				//r.closeRegionOperation();
//				r.startRegionOperation();
//				LOG.info("Split Start");
//				String arg = "bin/hbase org.apache.hadoop.hbase.util.RegionSplitter -r -o 2 test UniformSplit";
//				String[] args = new String[]{"bin/hbase org.apache.hadoop.hbase.util.RegionSplitter", "-r", "-o", "2", "test", "UniformSplit"};
//				try {
//					RegionSplitter.main(args);
//					LOG.info("Split Complete");
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					LOG.info("Split Fail");
//					e.printStackTrace();
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					LOG.info("Split Fail");
//					e.printStackTrace();
//				}
//				
//				r.closeRegionOperation();
				
				
//				SplitTransactionImpl splitter = new SplitTransactionImpl(r, splitKey);
//				if(splitter.prepare()){
//					try{
//						LOG.info("Split Complete");
//						User user = User.getCurrent();
//						PairOfSameType<Region> regions = splitter.execute((Server)ctx.getEnvironment().getRegionServerServices(), ctx.getEnvironment().getRegionServerServices(), user);
//						regions = splitter.stepsBeforePONR(arg0, arg1, arg2)
//					}catch(Exception e){
//						LOG.info("Split Fail");
//						e.printStackTrace();
//					}
//				}
				//r.closeRegionOperation();
			}
		}
	}

//	@Override
//	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, Region l, Region r) throws IOException {
//		super.postSplit(e, l, r);
//	}
//
//	@Override
//	public void postPut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
//			throws IOException {
//
//	}

}
