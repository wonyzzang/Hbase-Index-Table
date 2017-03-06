package ac.ku.milab.hbaseindex.coprocessor.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
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
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import ac.ku.milab.hbaseindex.IdxFilter;
import ac.ku.milab.hbaseindex.util.IdxConstants;
import ac.ku.milab.hbaseindex.util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	//private IdxManager indexManager = IdxManager.getInstance();

	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");

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
			byte[] leftRegionKey = l.getRegionInfo().getStartKey();
			byte[] rightRegionKey = r.getRegionInfo().getStartKey();
			
			String sIdxTableName = TableUtils.getIndexTableName(sTableName);
			TableName idxTableName = TableName.valueOf(sIdxTableName);
			
			List<Region> idxRegionList = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTableName);
			for(Region idxRegion : idxRegionList){
				byte[] regionKey = idxRegion.getRegionInfo().getStartKey();
				if(Bytes.contains(regionKey, leftRegionKey)){
					
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
			
			LOG.info("prePut processing - " + sCarNum + ","+lTime+","+dLat+","+dLon);

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
			Region idxRegion = idxRegions.get(0);
			idxRegion.put(idxPut);
		}

		LOG.info("PrePut END : " + tableName);

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
