package ac.ku.milab.hbaseindex.coprocessor.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.hbck.TableLockChecker;
import org.apache.hadoop.hbase.zookeeper.ZKTableStateManager;

import ac.ku.milab.hbaseindex.IdxManager;

public class IndexMasterObserver extends BaseMasterObserver {

	private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class.getName());

	private IdxManager indexManager = IdxManager.getInstance();

	// Calling this function before creating user table
	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
			HRegionInfo[] regions) throws IOException {
//		
//		String tableName = desc.getNameAsString();
//		
//		LOG.info("PreCreateTable START : " + tableName);
//		
//		if(TableUtils.isUserTable(Bytes.toBytes(tableName))){
//			MasterServices master = ctx.getEnvironment().getMasterServices();
//			Configuration conf = master.getConfiguration();
//			
//			//IdxHTableDescriptor Idesc = (IdxHTableDescriptor) desc;
//			
//			List<IdxColumnQualifier> idxColumns = new ArrayList<IdxColumnQualifier>();
//			IdxColumnQualifier col1 = new IdxColumnQualifier("q1", ValueType.String);
//			idxColumns.add(col1);
//			
//			indexManager.addIndexForTable(desc.getNameAsString(), idxColumns);
//
//			// consider only one column family
//			
//			String idxTableName = TableUtils.getIndexTableName(tableName);
//			
//			TableName idxTName = TableName.valueOf(idxTableName);
			
			// check if tables already exist

//			boolean isTableExist = MetaReader.tableExists(master.getCatalogTracker(), tableName);
//			boolean isIdxTableExist = MetaReader.tableExists(master.getCatalogTracker(), idxTableName);
//			if (isTableExist || isIdxTableExist) {
//				LOG.error("Table already exists");
//				throw new TableExistsException("Table " + tableName + " already exist.");
//			}

			// make index table
//			HTableDescriptor indextable = new HTableDescriptor(idxTName);
//			HColumnDescriptor indCol = new HColumnDescriptor(IdxConstants.IDX_FAMILY);
//			indextable.addFamily(indCol);
//			HRegionInfo[] hRegionInfos = new HRegionInfo[]{new HRegionInfo(idxTName, null, null)};
//			master.createTable(indextable, null, 0, 0);
//		}else{
//			LOG.info("System Table or Index Table");
//		}
//
//		LOG.info("PreCreateTable END");
	}

	// Calling this function before making user table disable
	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tName) throws IOException {
//		// TODO Auto-generated method stub
//
//		// get table name
//		MasterServices master = ctx.getEnvironment().getMasterServices();
//		String strTableName = tName.getNameAsString();
//		byte[] tableName = tName.getName();
//
//		// if table made disable is user table, index table is also disable
//		boolean isUserTable = TableUtils.isUserTable(tableName);
//		if (isUserTable) {
//			String idxTableName = TableUtils.getIndexTableName(strTableName);
//			TableName idxTName = TableName.valueOf(idxTableName);
//			master.disableTable(idxTName, 0, 0);
//		}
	}

	// Calling this function before making user table deleted
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tName) throws IOException {
//		// TODO Auto-generated method stub
//		// get table name
//		MasterServices master = ctx.getEnvironment().getMasterServices();
//		String strTableName = tName.getNameAsString();
//		byte[] tableName = tName.getName();
//
//		// if table made delete is user table, index table is also deleted
//		boolean isUserTable = TableUtils.isUserTable(tableName);
//		if (isUserTable) {
//			String idxTableName = TableUtils.getIndexTableName(strTableName);
//			TableName idxTName = TableName.valueOf(idxTableName);
//			master.deleteTable(idxTName, 0, 0);
//		}
	}

}
