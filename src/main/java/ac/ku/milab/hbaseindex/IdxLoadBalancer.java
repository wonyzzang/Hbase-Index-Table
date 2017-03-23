package ac.ku.milab.hbaseindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;

import ac.ku.milab.hbaseindex.util.IdxConstants;
import ac.ku.milab.hbaseindex.util.TableUtils;

public class IdxLoadBalancer extends StochasticLoadBalancer {
	
	private MasterServices master;

	private static final Log LOG = LogFactory.getLog(IdxLoadBalancer.class);

	private Map<String, Map<HRegionInfo, ServerName>> regionLocation = new ConcurrentHashMap<String, Map<HRegionInfo, ServerName>>();
	

	@Override
	public Configuration getConf() {
		return super.getConf();
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
	}
	

	@Override
	public void setClusterStatus(ClusterStatus cs) {
		super.setClusterStatus(cs);
	}

	@Override
	public void setMasterServices(MasterServices master) {
		this.master = master;
		super.setMasterServices(master);
	}
	
	
	@Override
	public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
		// TODO Auto-generated method stub
		LOG.info("Start balanceCluster");
		synchronized (this.regionLocation) {
			Map<ServerName, List<HRegionInfo>> userClusterState = new HashMap<ServerName, List<HRegionInfo>>(1);
			Map<ServerName, List<HRegionInfo>> indexClusterState = new HashMap<ServerName, List<HRegionInfo>>(1);
			boolean balanceByTable = true;
			String tableName = null;

			if (balanceByTable) {
				// Check and modify the regionLocation map based on values of
				// cluster state because we will
				// call balancer only when the cluster is in stable state and
				// reliable.
				Map<HRegionInfo, ServerName> regionMap = null;
				for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
					ServerName serverName = serverVsRegionList.getKey();
					List<HRegionInfo> regionInfoList = serverVsRegionList.getValue();
					if (regionInfoList.isEmpty()) {
						continue;
					}
					// Just get the table name from any one of the values in the
					// regioninfo list
					if (tableName == null) {
						tableName = regionInfoList.get(0).getTable().getNameAsString();
						regionMap = this.regionLocation.get(tableName);
					}
					if (regionMap != null) {
						for (HRegionInfo regionInfo : regionInfoList) {
							updateServer(regionMap, serverName, regionInfo);
						}
					}
				}
			} else {
				for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
					ServerName serverName = serverVsRegionList.getKey();
					List<HRegionInfo> regionsInfoList = serverVsRegionList.getValue();
					List<HRegionInfo> idxRegionsToBeMoved = new ArrayList<HRegionInfo>();
					List<HRegionInfo> uRegionsToBeMoved = new ArrayList<HRegionInfo>();
					for (HRegionInfo regionInfo : regionsInfoList) {
						if (regionInfo.isMetaRegion() || regionInfo.isSystemTable()) {
							continue;
						}
						tableName = regionInfo.getTable().getNameAsString();
						// table name may change every time thats why always
						// need to get table entries.
						Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
						if (regionMap != null) {
							updateServer(regionMap, serverName, regionInfo);
						}
						if (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
							idxRegionsToBeMoved.add(regionInfo);
							continue;
						}
						uRegionsToBeMoved.add(regionInfo);
					}
					// there may be dummy entries here if assignments by table
					// is set
					userClusterState.put(serverName, uRegionsToBeMoved);
					indexClusterState.put(serverName, idxRegionsToBeMoved);
				}
			}
			/*
			 * In case of table wise balancing if balanceCluster called for
			 * index table then no user regions available. At that time skip
			 * default balancecluster call and get region plan from region
			 * location map if exist.
			 */
			// TODO : Needs refactoring here
			List<RegionPlan> regionPlanList = null;

			if (balanceByTable && (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) == false) {
				regionPlanList = super.balanceCluster(clusterState);
				
				// regionPlanList is null means skipping balancing.
				if (regionPlanList == null) {
					return null;
				} else {
					saveRegionPlanList(regionPlanList);
					return regionPlanList;
				}
			} else if (balanceByTable && (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) == true) {
				regionPlanList = new ArrayList<RegionPlan>(1);
				String actualTableName = TableUtils.extractTableName(tableName);
				Map<HRegionInfo, ServerName> regionMap = regionLocation.get(actualTableName);
				// no previous region plan for user table.
				if (regionMap == null) {
					return null;
				}
				for (Entry<HRegionInfo, ServerName> entry : regionMap.entrySet()) {
					regionPlanList.add(new RegionPlan(entry.getKey(), null, entry.getValue()));
				}
				// for preparing the index plan
				List<RegionPlan> indexPlanList = new ArrayList<RegionPlan>(1);
				// copy of region plan to iterate.
				List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(regionPlanList);
				return prepareIndexPlan(clusterState, indexPlanList, regionPlanListCopy);
			} else {
				regionPlanList = super.balanceCluster(userClusterState);
				
				if (regionPlanList == null) {
					regionPlanList = new ArrayList<RegionPlan>(1);
				} else {
					saveRegionPlanList(regionPlanList);
				}
				List<RegionPlan> userRegionPlans = new ArrayList<RegionPlan>(1);

				for (Entry<String, Map<HRegionInfo, ServerName>> tableVsRegions : this.regionLocation.entrySet()) {
					Map<HRegionInfo, ServerName> regionMap = regionLocation.get(tableVsRegions.getKey());
					// no previous region plan for user table.
					if (regionMap == null) {
					} else {
						for (Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
							userRegionPlans.add(new RegionPlan(e.getKey(), null, e.getValue()));
						}
					}
				}

				List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(userRegionPlans);
				return prepareIndexPlan(indexClusterState, regionPlanList, regionPlanListCopy);
			}
		}
	}
	
	@Override
	public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
		LOG.info("Start randomAssignment");
		return super.randomAssignment(regionInfo, servers);
	}
	
	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regionList,
			List<ServerName> serverList) {
		LOG.info("Start roundRobinAssignment");
		List<HRegionInfo> userRegions = new ArrayList<HRegionInfo>();
		List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>();

		// separate user table and index table
		for (HRegionInfo regionInfo : regionList) {
			classifyRegion(regionInfo, userRegions, indexRegions);
		}

		Map<ServerName, List<HRegionInfo>> plan = null;
		if (userRegions.isEmpty() == false) {
			plan = super.roundRobinAssignment(userRegions, serverList);
			
			if (plan == null) {
				LOG.info("No region plan for user regions.");
				return null;
			}

			// save assignment plan
			synchronized (this.regionLocation) {
				savePlan(plan);
			}
		}
		plan = prepareIndexRegionPlan(indexRegions, plan, serverList);
		printPlan(plan);
		return plan;
	}
	
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regionList,
			List<ServerName> serverList) {
		LOG.info("Start retainAssignment");
		Map<HRegionInfo, ServerName> userRegionsMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
		List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);
		
		for (Entry<HRegionInfo, ServerName> entry : regionList.entrySet()) {
			classifyRegion(entry, userRegionsMap, indexRegions, serverList);
		}
		
		Map<ServerName, List<HRegionInfo>> plan = null;
		if (userRegionsMap.isEmpty() == false) {
			plan = super.retainAssignment(userRegionsMap, serverList);
			if (plan == null) {
				return null;
			}
			
			synchronized (this.regionLocation) {
				savePlan(plan);
			}
		}
		plan = prepareIndexRegionPlan(indexRegions, plan, serverList);
		return plan;
	}


	/**
	 * @param regionMap
	 *            map of servers and regions
	 * @param serverName
	 *            name of server
	 * @param regionInfo
	 *            region
	 */

	private void updateServer(Map<HRegionInfo, ServerName> regionMap, ServerName serverName, HRegionInfo regionInfo) {
		ServerName existingServer = regionMap.get(regionInfo);
		if (!serverName.equals(existingServer)) {
			regionMap.put(regionInfo, serverName);
		}
	}

	/**
	 * @param indexClusterState
	 *            map of servers and regions
	 * @param regionPlanList
	 *            name of server
	 * @param regionPlanListCopy
	 *            region
	 * @return list of region plan
	 */

	// Creates the index region plan based on the corresponding user region plan
	private List<RegionPlan> prepareIndexPlan(Map<ServerName, List<HRegionInfo>> indexClusterState,
			List<RegionPlan> regionPlanList, List<RegionPlan> regionPlanListCopy) {

		for (RegionPlan regionPlan : regionPlanListCopy) {
			HRegionInfo regionInfo = regionPlan.getRegionInfo();

			for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : indexClusterState.entrySet()) {
				List<HRegionInfo> indexRegions = serverVsRegionList.getValue();
				ServerName server = serverVsRegionList.getKey();
				if (regionPlan.getDestination().equals(server)) {
					// desination server in the region plan is new and should
					// not be same with this
					// server in index cluster state.thats why skipping regions
					// check in this server
					continue;
				}
				String actualTableName = null;

				boolean isExist = false;
				HRegionInfo indexRegionInfo = null;
				for (int i = 0; i < indexRegions.size(); i++) {
					indexRegionInfo = indexRegions.get(i);
					String indexTableName = indexRegionInfo.getTable().getNameAsString();
					actualTableName = TableUtils.extractTableName(indexTableName);
					LOG.info("indextable : "+indexTableName);
					LOG.info("actualtable : "+actualTableName);
					if (regionInfo.getTable().getNameAsString().equals(actualTableName) == false) {
						continue;
					}
//					if (Bytes.compareTo(regionInfo.getStartKey(), indexRegionInfo.getStartKey()) != 0) {
//						continue;
//					}
					if(indexRegionInfo.getStartKey()==null){
						LOG.info("i00000");
					}else{
						LOG.info("ilength-"+indexRegionInfo.getStartKey());
					}
					
					if(regionInfo.getStartKey()==null){
						LOG.info("r00000");
					}else{
						LOG.info("rlength-"+regionInfo.getStartKey());
					}
					
					if(!Bytes.contains(indexRegionInfo.getStartKey(), regionInfo.getStartKey())){
						continue;
					}
					isExist = true;
					break;
				}
				if (isExist) {
					RegionPlan rp = new RegionPlan(indexRegionInfo, server, regionPlan.getDestination());

					putRegionPlan(indexRegionInfo, regionPlan.getDestination());
					regionPlanList.add(rp);
				}
				continue;
			}
		}
		for (RegionPlan plan : regionPlanList) {

		}
		regionPlanListCopy.clear();
		// if no user regions to balance then return newly formed index region
		// plan.

		return regionPlanList;
	}
	
//	private List<RegionPlan> prepareIndexPlan(Map<ServerName, List<HRegionInfo>> indexClusterState,
//			List<RegionPlan> regionPlanList, List<RegionPlan> regionPlanListCopy) {
//
//		OUTER_LOOP: for (RegionPlan regionPlan : regionPlanListCopy) {
//			HRegionInfo regionInfo = regionPlan.getRegionInfo();
//
//			MIDDLE_LOOP: for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : indexClusterState.entrySet()) {
//				List<HRegionInfo> indexRegions = serverVsRegionList.getValue();
//				ServerName server = serverVsRegionList.getKey();
//				if (regionPlan.getDestination().equals(server)) {
//					// desination server in the region plan is new and should
//					// not be same with this
//					// server in index cluster state.thats why skipping regions
//					// check in this server
//					continue MIDDLE_LOOP;
//				}
//				String actualTableName = null;
//
//				for (HRegionInfo indexRegionInfo : indexRegions) {
//					String indexTableName = indexRegionInfo.getTable().getNameAsString();
//					actualTableName = TableUtils.extractTableName(indexTableName);
//					if (regionInfo.getTable().getNameAsString().equals(actualTableName) == false) {
//						continue;
//					}
//					if (Bytes.compareTo(regionInfo.getStartKey(), indexRegionInfo.getStartKey()) != 0) {
//						continue;
//					}
//					RegionPlan rp = new RegionPlan(indexRegionInfo, server, regionPlan.getDestination());
//
//					putRegionPlan(indexRegionInfo, regionPlan.getDestination());
//					regionPlanList.add(rp);
//					continue OUTER_LOOP;
//				}
//			}
//		}
//		regionPlanListCopy.clear();
//		// if no user regions to balance then return newly formed index region
//		// plan.
//
//		return regionPlanList;
//	}

	/**
	 * @param regionPlanList
	 *            map of servers and regions
	 * @return list of region plan
	 */

	private void saveRegionPlanList(List<RegionPlan> regionPlanList) {
		for (RegionPlan regionPlan : regionPlanList) {
			HRegionInfo regionInfo = regionPlan.getRegionInfo();
			putRegionPlan(regionInfo, regionPlan.getDestination());
		}
	}

	/**
	 * @param plan
	 *            allocation plan of regions
	 * @return
	 */

	private void savePlan(Map<ServerName, List<HRegionInfo>> plan) {

		// regionlocation saves plan
		for (Entry<ServerName, List<HRegionInfo>> entry : plan.entrySet()) {
			for (HRegionInfo region : entry.getValue()) {
				putRegionPlan(region, entry.getKey());
			}
			LOG.info("Saved user regions' plans for server " + entry.getKey() + '.');
		}
	}

	/**
	 * @param regionInfo
	 *            region info of region that we want to know whether it is user
	 *            table or index table
	 * @param userRegions
	 *            user region list
	 * @param indexRegions
	 *            index region list
	 * @return
	 */

	private void classifyRegion(HRegionInfo regionInfo, List<HRegionInfo> userRegions, List<HRegionInfo> indexRegions) {
		// if table name has index table suffix, table is index table
		// otherwise, table is user table
		if (regionInfo.getTable().getNameAsString().endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			indexRegions.add(regionInfo);
		} else {
			userRegions.add(regionInfo);
		}
	}

	private void classifyRegion(Entry<HRegionInfo, ServerName> entry, Map<HRegionInfo, ServerName> userRegionsMap,
			List<HRegionInfo> indexRegions, List<ServerName> serverList) {

		HRegionInfo regionInfo = entry.getKey();
		if (regionInfo.getTable().getNameAsString().endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			indexRegions.add(regionInfo);
			return;
		}
		if (entry.getValue() == null) {
			Random rand = new Random(System.currentTimeMillis());
			userRegionsMap.put(regionInfo, serverList.get(rand.nextInt(serverList.size())));
		} else {
			userRegionsMap.put(regionInfo, entry.getValue());
		}
	}

	/**
	 * @param regionInfo
	 *            regionInfo of table
	 * @param serverName
	 *            server name which saves region
	 * @return
	 */

	public void putRegionPlan(HRegionInfo regionInfo, ServerName serverName) {
		String tableName = regionInfo.getTable().getNameAsString();
		synchronized (this.regionLocation) {
			// get region map of table
			Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);

			// if region map is null, table's regions are not allocated before
			// so, put new map element for table
			if (regionMap == null) {
				LOG.info("No regions of table in region plan");
				regionMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
				this.regionLocation.put(tableName, regionMap);
			}
			LOG.info("put Region Plan - " + regionInfo.getRegionNameAsString() + "," + serverName);
			regionMap.put(regionInfo, serverName);
		}
	}

	/**
	 * @param indexRegions
	 *            regionInfo of index table's region
	 * @param plan
	 *            assignment plan
	 * @param serverList
	 *            region server list
	 * @return plan including index table region allocation plan
	 */

	private Map<ServerName, List<HRegionInfo>> prepareIndexRegionPlan(List<HRegionInfo> indexRegions,
			Map<ServerName, List<HRegionInfo>> plan, List<ServerName> serverList) {
		LOG.info("prepareIndexRegionPlan start");
		// if index regions don't exist, return plan
		if (indexRegions != null && indexRegions.isEmpty() == false) {
			if (plan == null) {
				plan = new ConcurrentHashMap<ServerName, List<HRegionInfo>>(1);
			}
			for (HRegionInfo regionInfo : indexRegions) {

				// get server name that has user region whose start key is same
				// with index region
				ServerName destServer = getServerNameForIdxRegion(regionInfo);
				List<HRegionInfo> destServerRegions = null;
				
				

				// if can't find server, random assign region
				if (destServer == null) {
					LOG.info("Cannot find");
					destServer = this.randomAssignment(regionInfo, serverList);
					destServerRegions = plan.get(destServer);
					if (destServerRegions == null) {
						destServerRegions = new ArrayList<HRegionInfo>(1);
						plan.put(destServer, destServerRegions);
					}
					destServerRegions.add(regionInfo);
				}

				// otherwise, assign region to server
				else {
					destServerRegions = plan.get(destServer);
					if (destServerRegions == null) {
						destServerRegions = new ArrayList<HRegionInfo>(1);
						plan.put(destServer, destServerRegions);
					}
					destServerRegions.add(regionInfo);
				}
			}
		}
		return plan;
	}

	
	/**
	 * @param regionInfo
	 *            regionInfo of index table's region
	 * @return name of server which contains user table region that has same
	 *         start key
	 */

	private ServerName getServerNameForIdxRegion(HRegionInfo regionInfo) {
		LOG.info("getServerNameForIdxRegion Start");
		String indexTableName = regionInfo.getTable().getNameAsString();
		String userTableName = TableUtils.extractTableName(indexTableName);

		synchronized (this.regionLocation) {
			// get region and server of user table
			LOG.info("getServerNameForIdxRegion Synch");
			Map<HRegionInfo, ServerName> regionMap = regionLocation.get(userTableName);
			if (regionMap == null) {
				LOG.info("getServerNameForIdxRegion Null");
				return null;
			}

			// check start key of all regions
			// return server name if find region start key matched
			LOG.info("getServerNameForIdxRegion loop");
			Map<ServerName, Integer> numberOfRegions = new HashMap<ServerName,Integer>();
			for (Map.Entry<HRegionInfo, ServerName> entry : regionMap.entrySet()) {
				ServerName name = entry.getValue();
				if(numberOfRegions.containsKey(name)){
					int num = numberOfRegions.get(name);
					num++;
					numberOfRegions.put(name, num);
				}else{
					numberOfRegions.put(name, 1);
				}

			}
			
			Map<HRegionInfo, ServerName> idxRegionMap = regionLocation.get(indexTableName);
			if(idxRegionMap==null){
				ServerName sn = regionMap.entrySet().iterator().next().getValue();
				putRegionPlan(regionInfo, sn);
				return sn;
				
			}
			
			Map<ServerName, Integer> numberOfIdxRegions = new HashMap<ServerName,Integer>();
			for (Map.Entry<HRegionInfo, ServerName> entry : idxRegionMap.entrySet()) {
				ServerName name = entry.getValue();
				if(numberOfIdxRegions.containsKey(name)){
					int num = numberOfIdxRegions.get(name);
					num++;
					numberOfIdxRegions.put(name, num);
				}else{
					numberOfIdxRegions.put(name, 1);
				}
			}
			
			for(ServerName sn : numberOfRegions.keySet()){
				if(numberOfIdxRegions.get(sn)==null){
					if(numberOfRegions.get(sn)>0){
						putRegionPlan(regionInfo, sn);
						return sn;
					}
				}
				if(numberOfRegions.get(sn)<=numberOfIdxRegions.get(sn)){
					continue;
				}else{
					putRegionPlan(regionInfo, sn);
					return sn;
				}
			}
		}
		return null;
	}
	
	private void printPlan(Map<ServerName, List<HRegionInfo>> plan){
		Set<ServerName> setServer = plan.keySet();
		Iterator<ServerName> iter = setServer.iterator();
		while(iter.hasNext()){
			ServerName name = iter.next();
			List<HRegionInfo> list = plan.get(name);
			for(HRegionInfo regionInfo : list){
				LOG.info("print plan "+name+" : " + regionInfo.getTable().getNameAsString());
			}
			
		}
		
	}

}
