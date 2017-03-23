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
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.SplitTransactionFactory;
import org.apache.hadoop.hbase.regionserver.SplitTransactionImpl;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KeyRange;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.RegionSplitCalculator;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.RpcController;

import ac.ku.milab.hbaseindex.IdxFilter;
import ac.ku.milab.hbaseindex.ZOrder;
import ac.ku.milab.hbaseindex.util.IdxConstants;
import ac.ku.milab.hbaseindex.util.TableUtils;
import io.netty.handler.ssl.OpenSslServerContext;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	//private IdxManager indexManager = IdxManager.getInstance();

	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");
	private static int cnt = 0;
	private static final byte[] BYTE_NULL = Bytes.toBytes("");
	
	private static int regionNum = 0;

//	@Override
//	public void stop(CoprocessorEnvironment e) throws IOException {
//		// nothing to do here
//	}
	
//	@Override
//	public void start(CoprocessorEnvironment e) throws IOException {
//		// TODO Auto-generated method stub
//		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment)e;
//		
//		TableName tName = TableName.valueOf("test");
//		TableName idxName = TableName.valueOf("test_idx");
//		
//		byte[] regionKey = env.getRegionInfo().getStartKey();
//		byte[] tableName = env.getRegionInfo().getTable().getName();
//		
//		boolean isUserTable = TableUtils.isUserTable(tableName);
//		boolean isIndexTable = TableUtils.isIndexTable(tableName);
//		
//		if(isUserTable){
//			List<Region> tableList = env.getRegionServerServices().getOnlineRegions(tName);
//		}
//		
//		
//		List<Region> idxTableList = env.getRegionServerServices().getOnlineRegions(idxName);
//		
//		if(tableList.size()!=0){
//			for(Region r : tableList){
//				
//			}
//		}
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
			LOG.info("preStoreScannerOpen Region : " + ctx.getEnvironment().getRegionInfo().getRegionId());
			Filter f = scan.getFilter();
			boolean isIndexFilter = (f instanceof IdxFilter);
		if(f!=null && isIndexFilter){
			
			String idxTableName = TableUtils.getIndexTableName(tableName);
			TableName idxTName = TableName.valueOf(idxTableName);
			
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			if(idxRegions.size()==0){
				return null;
			}else{
				int id = getRegionNumber(ctx.getEnvironment());
				Region idxRegion = null;
				for(int i=0;i<idxRegions.size();i++){
					idxRegion = idxRegions.get(i);
					CoprocessorEnvironment env = idxRegion.getCoprocessorHost().findCoprocessorEnvironment("ac.ku.milab.hbaseindex.coprocessor.regionserver.IndexRegionObserver");
					int num = getRegionNumber((RegionCoprocessorEnvironment)env);
					if(id==num){
						break;
					}
				}
				
				double lat1 = 80.4;
				double lon1 = 100.3;
				
				double lat2 = 81.1;
				double lon2 = 101.7;
				
				int code1 = ZOrder.Encode(lat1, lon1);
				int code2 = ZOrder.Encode(lat2+1, lon2+1);
				
				Scan indScan = new Scan();
				
				Map<byte[], NavigableSet<byte[]>> map = indScan.getFamilyMap();
				 NavigableSet<byte[]> indCols = map.get(Bytes.toBytes("IND"));
				 Store indStore = idxRegion.getStore(Bytes.toBytes("IND"));
				 ScanInfo scanInfo = null;
				 scanInfo = indStore.getScanInfo();
				 long ttl = scanInfo.getTtl();
				 
				 scanInfo = new ScanInfo(scanInfo.getConfiguration(),
						 indStore.getFamily(), ttl,
						 scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
				LOG.info("well done");
				ctx.complete();
						 
				return new StoreScanner(indStore, scanInfo, indScan, indCols, ((HStore)indStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
				}
			
			
			
			//LOG.info("filter string : " + f.toString());
			//Filter indFilter;
			//Filter indFilter = new RowFilter(CompareOp.EQUAL, new
			//BinaryComparator(Bytes.toBytes("idx1v1row1")));
			
			//LOG.info("preStoreScannerOpen User table : " + tableName + " & " +
			// idxTableName);
			
			 
//			 indScan.setStartRow(Bytes.toBytes(code1));
//			 indScan.setStopRow(Bytes.toBytes(code2));
//			 scan.setStartRow(Bytes.toBytes(code1));
//			 scan.setStopRow(Bytes.toBytes(code2));
			 //indScan.setStartRow(Bytes.toBytes("idx1v1"));
			 //indScan.setStopRow(Bytes.toBytes("idx1v2"));
			 //indScan.setFilter(indFilter);
			 
			
			 //LOG.info("filter string : " + indScan.getFilter().toString());
			
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
//	
//	@Override
//	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
//		// TODO Auto-generated method stub
//		super.preSplit(c, splitRow);
//	}
	
	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		// TODO Auto-generated method stub
		TableName tableName = ctx.getEnvironment().getRegionInfo().getTable();
		String sTableName = tableName.getNameAsString();
		
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(sTableName));
		
		if(isUserTable){
			RegionServerServices rsService = ctx.getEnvironment().getRegionServerServices();
			ServerName serverName = rsService.getServerName();
			
			int regionNum = getRegionNumber(ctx.getEnvironment());
			
			String sIdxTableName = TableUtils.getIndexTableName(sTableName);
			TableName idxTableName = TableName.valueOf(sIdxTableName);
			
			List<Region> idxRegionList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTableName);
			Region idxRegion = null;
			for(int i=0;i<idxRegionList.size();i++){
				idxRegion = idxRegionList.get(i);
				CoprocessorEnvironment env = idxRegion.getCoprocessorHost().findCoprocessorEnvironment("ac.ku.milab.hbaseindex.coprocessor.regionserver.IndexRegionObserver");
				int idxRegionNum = getRegionNumber((RegionCoprocessorEnvironment)env);
				
				if(regionNum == idxRegionNum){
					break;
				}
			}
			
			if(idxRegion!=null){
				HexStringSplit split = new HexStringSplit();
				//UniformSplit split = new RegionSplitter.UniformSplit();
//				HRegion hRegion = (HRegion) idxRegion;
				List<Store> stores = idxRegion.getStores();
				byte[] splitPointFromLargestStore = null;
				    long largestStoreSize = 0;
				    for (Store s : stores) {
				      byte[] splitPoint = s.getSplitPoint();
				      long storeSize = s.getSize();
				      if (splitPoint != null && largestStoreSize < storeSize) {
				        splitPointFromLargestStore = splitPoint;
				        largestStoreSize = storeSize;
				      }
				    }
				//byte[][] splitKey = split.split(2);
				    SplitTransactionImpl splitter = new SplitTransactionImpl(idxRegion, splitPointFromLargestStore);
				//SplitTransactionImpl splitter = new SplitTransactionImpl(idxRegion, splitKey[0]);
				if(splitter.prepare()){
					try{
						splitter.execute((Server)rsService, rsService);
						LOG.info("Split Complete");
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}

		}
	}
//	@Override
//	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> ctx, Region l, Region r) throws IOException {
//		// TODO Auto-generated method stub
//		TableName tableName = ctx.getEnvironment().getRegionInfo().getTable();
//		String sTableName = tableName.getNameAsString();
//		
//		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(sTableName));
//		
//		if(isUserTable){
//			RegionServerServices rsService = ctx.getEnvironment().getRegionServerServices();
//			ServerName serverName = rsService.getServerName();
//			
//			byte[] leftRegionKey = l.getRegionInfo().getStartKey();
//			byte[] rightRegionKey = r.getRegionInfo().getStartKey();
//			LOG.info("left key is" + leftRegionKey);
//			LOG.info("right key is" + rightRegionKey);
//			
//			String sIdxTableName = TableUtils.getIndexTableName(sTableName);
//			TableName idxTableName = TableName.valueOf(sIdxTableName);
//			
//			List<Region> idxRegionList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTableName);
//			
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
//			
//			for(Region idxRegion :idxRegionList){
//				
//			}
//			
//			for(Region idxRegion : idxRegionList){
//				byte[] regionKey = idxRegion.getRegionInfo().getStartKey();
//				//if(Bytes.contains(regionKey, leftRegionKey)){
//					//HexStringSplit splitter = new RegionSplitter.HexStringSplit();
//					//splitter.split(leftRegionKey, rightRegionKey);
//					byte[] startKey = idxRegion.getRegionInfo().getStartKey();
//					
//					byte[] endKey = idxRegion.getRegionInfo().getEndKey();
//					UniformSplit split = new RegionSplitter.UniformSplit();
//					
//					byte[][] splitKey = split.split(2);
//					
//					SplitTransactionImpl splitter = new SplitTransactionImpl(idxRegion, splitKey[0]);
//					if(splitter.prepare()){
//						try{
//							splitter.execute((Server)rsService, rsService);
//							LOG.info("Split Complete");
//						}catch(Exception e){
//							e.printStackTrace();
//						}
//					}
//					
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
//				}
//			}
//		}
//	}
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

//			Cell numCell = list.get(0);
//			carNum = CellUtil.cloneValue(numCell);
//
//			Cell timeCell = list.get(1);
//			time = CellUtil.cloneValue(timeCell);
//
//			Cell latCell = list.get(2);
//			lat = CellUtil.cloneValue(latCell);
//
//			Cell lonCell = list.get(3);
//			lon = CellUtil.cloneValue(lonCell);
			
			Cell latCell = list.get(0);
			lat = CellUtil.cloneValue(latCell);

			Cell lonCell = list.get(1);
			lon = CellUtil.cloneValue(lonCell);
			
			byte[] rowKey = put.getRow();
			carNum = Bytes.copy(rowKey, 0, 9);
			time = Bytes.copy(rowKey, 9, 8);
			//byte[] indexRowKey = Bytes.add(carNum, time);
			//indexRowKey = Bytes.add(indexRowKey, lat, lon);

//			String sCarNum = Bytes.toString(carNum);
//			long lTime = Bytes.toLong(time);
			double dLat = Bytes.toDouble(lat);
			double dLon = Bytes.toDouble(lon);
			
			int code = ZOrder.Encode(dLat, dLon);
			byte[] bCode = Bytes.toBytes(code);
			
			byte[] indexRowKey = Bytes.add(bCode, time, carNum);
			indexRowKey = Bytes.add(indexRowKey, lat, lon);
			
			
			
			//LOG.info("prePut processing - " + sCarNum + ","+lTime+","+dLat+","+dLon);

			// get index column
//			List<IdxColumnQualifier> idxColumns = indexManager.getIndexOfTable(tableName);
//			for (IdxColumnQualifier cq : idxColumns) {
//				LOG.info("index column : " + cq.getQualifierName());
//			}

			// get region
//			HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
//			Region region = ctx.getEnvironment().getRegion();

			/*
			 * index table rowkey = region start key + "idx" + all(qualifier
			 * number + value)
			 */

			// get region start keys
//			byte[] startKey = hRegionInfo.getStartKey();
//			indexRowKey = Bytes.add(startKey, indexRowKey);
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
			
//			HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
//			idxTable.put(idxPut);
			// index table and put
//			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//			int size = idxRegions.size();
//			boolean isInThisRS = false;
//			for(int i=0;i<size;i++){
//				Region r = idxRegions.get(i);
//				byte[] idxStartKey = r.getRegionInfo().getStartKey();
//				int compareRes = Bytes.compareTo(indexRowKey, idxStartKey);
//				if(compareRes!=-1){
//					if(i==size-1||compareRes==0){
//						//LOG.info("index OK");
//						r.put(idxPut);
//						isInThisRS = true;
//					}else{
//						Region nextRegion = idxRegions.get(i+1);
//						byte[] idxNextKey = nextRegion.getRegionInfo().getStartKey();
//						if(Bytes.compareTo(indexRowKey, idxNextKey)==-1){
//							//LOG.info("index OK");
//							r.put(idxPut);
//							isInThisRS = true;
//						}
//					}
//				}
//			}
//			cnt++;
//			if(cnt==100){
//			}
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			int size = idxRegions.size();
			boolean isInThisRS = false;
			for(int i=0;i<size;i++){
				Region r = idxRegions.get(i);
				byte[] idxStartKey = r.getRegionInfo().getStartKey();
				byte[] idxEndKey = r.getRegionInfo().getEndKey();
				if(Bytes.compareTo(idxStartKey, BYTE_NULL)==0){
					if(Bytes.compareTo(idxEndKey, BYTE_NULL)==0){
						r.put(idxPut);
						isInThisRS = true;
					}else if(Bytes.compareTo(idxEndKey, indexRowKey)>0){
						r.put(idxPut);
						isInThisRS = true;
					}
				}else if(Bytes.compareTo(idxStartKey, BYTE_NULL)>0){
					if(Bytes.compareTo(idxStartKey, indexRowKey)<0){
						if(Bytes.compareTo(idxEndKey, BYTE_NULL)==0){
							r.put(idxPut);
							isInThisRS = true;
						}else if(Bytes.compareTo(idxEndKey, indexRowKey)>0){
							r.put(idxPut);
							isInThisRS = true;
						}
					}else{
						
					}
				}
			}
//			
//			
			if(isInThisRS==false){
				HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
				idxTable.put(idxPut);
			}
//			if(isInThisRS==false){
//				//MetaTableAccessor accessor = new MetaTableAccessor();
//				List<Pair<HRegionInfo, ServerName>> idxRegionList = MetaTableAccessor.getTableRegionsAndLocations(new ZooKeeperWatcher(ctx.getEnvironment().getConfiguration(), null, null), ConnectionFactory.createConnection(), idxTName);
//
//				ServerName serverHere = ctx.getEnvironment().getRegionServerServices().getServerName();
//
//				for(Pair<HRegionInfo, ServerName> pair : idxRegionList){
//					ServerName server = pair.getSecond();
//					
//					if(server.equals(serverHere)){
//						continue;
//					}else{
//						HRegionInfo info = pair.getFirst();
//
//						byte[] idxStartKey = info.getStartKey();
//						byte[] idxEndKey = info.getEndKey();
//						if(Bytes.compareTo(idxStartKey, BYTE_NULL)==0){
//							if(Bytes.compareTo(idxEndKey, BYTE_NULL)==0){
//								List<Region> idxList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//								if(idxList == null || idxList.size()==0){
//									HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
//									HTableDescriptor desc = idxTable.getTableDescriptor();
//									byte[] identifier = Bytes.toBytes("idx");
//									WAL wal = WALFactory.getInstance(idxTable.getConfiguration()).getWAL(identifier);
//									HRegion hRegion = HRegion.openHRegion(info, desc, wal, idxTable.getConfiguration());
//									hRegion.put(idxPut);
//								}else{
//									HTableDescriptor desc = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName).get(0).getTableDesc();
//									HRegion hRegion = HRegion.openHRegion(info, desc, ctx.getEnvironment().getRegionServerServices().getWAL(info), ctx.getEnvironment().getConfiguration());
//									hRegion.put(idxPut);
//								}	
//							}else if(Bytes.compareTo(idxEndKey, indexRowKey)>0){
//								List<Region> idxList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//								if(idxList == null|| idxList.size()==0){
//									HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
//									HTableDescriptor desc = idxTable.getTableDescriptor();
//									byte[] identifier = Bytes.toBytes("idx");
//									
//									WAL wal = WALFactory.getInstance(idxTable.getConfiguration()).getWAL(identifier);
//									HRegion hRegion = HRegion.openHRegion(info, desc, wal, idxTable.getConfiguration());
//									hRegion.put(idxPut);
//								}else{
//									HTableDescriptor desc = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName).get(0).getTableDesc();
//									HRegion hRegion = HRegion.openHRegion(info, desc, ctx.getEnvironment().getRegionServerServices().getWAL(info), ctx.getEnvironment().getConfiguration());
//									hRegion.put(idxPut);
//								}
//							}
//						}else if(Bytes.compareTo(idxStartKey, BYTE_NULL)>0){
//							if(Bytes.compareTo(idxEndKey, BYTE_NULL)==0){
//								List<Region> idxList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//								if(idxList == null|| idxList.size()==0){
//									HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
//									HTableDescriptor desc = idxTable.getTableDescriptor();
//									byte[] identifier = Bytes.toBytes("idx");
//									WAL wal = WALFactory.getInstance(idxTable.getConfiguration()).getWAL(identifier);
//									HRegion hRegion = HRegion.openHRegion(info, desc, wal, idxTable.getConfiguration());
//									hRegion.put(idxPut);
//								}else{
//									HTableDescriptor desc = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName).get(0).getTableDesc();
//									HRegion hRegion = HRegion.openHRegion(info, desc, ctx.getEnvironment().getRegionServerServices().getWAL(info), ctx.getEnvironment().getConfiguration());
//									hRegion.put(idxPut);
//								}
//							}else if(Bytes.compareTo(idxEndKey, indexRowKey)>0){
//								List<Region> idxList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
//								if(idxList == null|| idxList.size()==0){
//									HTable idxTable = new HTable(ctx.getEnvironment().getConfiguration(), idxTName);
//									HTableDescriptor desc = idxTable.getTableDescriptor();
//									byte[] identifier = Bytes.toBytes("idx");
//									WAL wal = WALFactory.getInstance(idxTable.getConfiguration()).getWAL(identifier);
//									HRegion hRegion = HRegion.openHRegion(info, desc, wal, idxTable.getConfiguration());
//									hRegion.put(idxPut);
//								}else{
//									HTableDescriptor desc = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName).get(0).getTableDesc();
//									HRegion hRegion = HRegion.openHRegion(info, desc, ctx.getEnvironment().getRegionServerServices().getWAL(info), ctx.getEnvironment().getConfiguration());
//									hRegion.put(idxPut);
//								}
//							}
//						}
//					}
//				}
//		}
//			CoordinatedStateManager manager = CoordinatedStateManagerFactory.getCoordinatedStateManager(ctx.getEnvironment().getConfiguration());
//			try {
//				HMaster master = new HMaster(ctx.getEnvironment().getConfiguration(), manager);
//				GetOnlineRegionRequest request = RequestConverter.buildGetOnlineRegionRequest();
//				MetaTableAccessor accessor = new MetaTableAccessor();
//				List<Pair<HRegionInfo, ServerName>> idxRegionList = accessor.getTableRegionsAndLocations(new ZooKeeperWatcher(ctx.getEnvironment().getConfiguration(), null, null), ConnectionFactory.createConnection(), idxTName);
//				ServerName serverHere = ctx.getEnvironment().getRegionServerServices().getServerName();
//				
//				for(Pair<HRegionInfo, ServerName> pair : idxRegionList){
//					ServerName server = pair.getSecond();
//					if(server.equals(serverHere)){
//						continue;
//					}else{
//						
//					}
//				}
//				RpcController controller = RpcControllerFactory.instantiate(ctx.getEnvironment().getConfiguration()).newController();
//				GetOnlineRegionResponse response = master.getMasterRpcServices().getOnlineRegion(controller, request);
//				List<RegionInfo> regionList = response.getRegionInfoList();
//				for(RegionInfo regionInfo : regionList){
//					HRegionInfo info = HRegionInfo.convert(regionInfo);
//					info.getse
//				}
				
				//new HTable(conf, tableName)
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
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
	
	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
			throws IOException {
		// TODO Auto-generated method stub
		TableName tName = e.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		
		
		// LOG.info("PrePut START : " + tableName);

		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			Filter f = scan.getFilter();
			boolean isIndexFilter = (f instanceof IdxFilter);
		if(f!=null && isIndexFilter){
			double lat1 = 34.0;
			double lon1 = 30.0;
			
			double lat2 = 94.0;
			double lon2 = 61.3;
			
			
			int code1 = ZOrder.Encode(lat1, lon1);
			int code2 = ZOrder.Encode(lat2+1, lon2+1);
			
			scan.setStartRow(Bytes.toBytes(code1));
			scan.setStopRow(Bytes.toBytes(code2));
			
			return super.preScannerOpen(e, scan, s);
		}
		
		}
		
		return super.preScannerOpen(e, scan, s);
	}
	
	public int getRegionNumber(RegionCoprocessorEnvironment env) throws IOException{
		TableName tName = env.getRegionInfo().getTable();
		byte[] tableName = tName.getName();
		
		boolean isUserTable = TableUtils.isUserTable(tableName);
		
		int number = 0;
		if(isUserTable){
			byte[] currentRegionKey = env.getRegionInfo().getStartKey();
			List<Region> regionList = env.getRegionServerServices().getOnlineRegions(tName);
			int size = regionList.size();
			
			for(int i=0;i<size;i++){
				Region r = regionList.get(i);
				byte[] regionKey = r.getRegionInfo().getStartKey();
				int compareRes = Bytes.compareTo(regionKey, currentRegionKey);
				if(compareRes<0){
					number++;
					
				}else{
				}
			}
			LOG.info("getRegionNumber " + env.getRegionInfo().getRegionNameAsString() +" : " + number);
			return number;
		}
		
		boolean isIndexTable = TableUtils.isIndexTable(tableName);
		if(isIndexTable){
			byte[] currentRegionKey = env.getRegionInfo().getStartKey();
			List<Region> regionList = env.getRegionServerServices().getOnlineRegions(tName);
			int size = regionList.size();
			
			for(int i=0;i<size;i++){
				Region r = regionList.get(i);
				byte[] regionKey = r.getRegionInfo().getStartKey();
				int compareRes = Bytes.compareTo(regionKey, currentRegionKey);
				if(compareRes<0){
					number++;
					
				}else{
				}
			}
			LOG.info("getRegionNumber " + env.getRegionInfo().getRegionNameAsString() +" : " + number);
			return number;
		}
		LOG.info("getRegionNumber " + env.getRegionInfo().getRegionNameAsString() +" : " + number);
		return number;
	}

}
