package ac.ku.milab.hbaseindex.coprocessor.wal;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseWALObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALKey;

import ac.ku.milab.hbaseindex.IdxColumnQualifier;
import ac.ku.milab.hbaseindex.IdxManager;
import ac.ku.milab.hbaseindex.util.TableUtils;


public class IndexWALObserver extends BaseWALObserver {

	private static final Log LOG = LogFactory.getLog(IndexWALObserver.class);

	private IdxManager indexManager = IdxManager.getInstance();

	@Override
	public void start(CoprocessorEnvironment arg0) throws IOException {
	}

	@Override
	public void stop(CoprocessorEnvironment arg0) throws IOException {
	}

	@Override
	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> arg0, HRegionInfo arg1, HLogKey arg2,
			WALEdit arg3) throws IOException {
	}

	@Override
	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo regionInfo, HLogKey logKey,
			WALEdit walEdit) throws IOException {
		String strTableName = regionInfo.getTable().getNameAsString();
		if (!TableUtils.isUserTable(Bytes.toBytes(strTableName))) {
			return true;
		}

		List<IdxColumnQualifier> idxColumns = indexManager.getIndexOfTable(strTableName);
		if (idxColumns != null && !idxColumns.isEmpty()) {
			LOG.trace("Entering preWALWrite for the table " + strTableName);
			String indexTableName = TableUtils.getIndexTableName(strTableName);
			// IndexEdits iEdits = IndexRegionObserver.threadLocal.get();
			// WALEdit indexWALEdit = iEdits.getWALEdit();
			// This size will be 0 when none of the Mutations to the user table
			// to be indexed.
			// or write to WAL is disabled for the Mutations
			// if (indexWALEdit.getKeyValues().size() == 0) {
			// return true;
			// }
			// LOG.trace("Adding indexWALEdits into WAL for table " +
			// tableNameStr);
			// HRegion indexRegion = iEdits.getRegion();
			// TS in all KVs within WALEdit will be the same. So considering the
			// 1st one.
			// Long time = indexWALEdit.getKeyValues().get(0).getTimestamp();
			// ctx.getEnvironment()
			// .getWAL()
			// .appendNoSync(indexRegion.getRegionInfo(),
			// Bytes.toBytes(indexTableName), indexWALEdit,
			// logKey.getClusterId(), time, indexRegion.getTableDesc());
			// LOG.trace("Exiting preWALWrite for the table " + tableNameStr);
		}
		return true;
	}

	@Override
	public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx, HRegionInfo hRegionInfo, WALKey key,
			WALEdit edit) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx, HRegionInfo hRegionInfo, WALKey key,
			WALEdit edit) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
