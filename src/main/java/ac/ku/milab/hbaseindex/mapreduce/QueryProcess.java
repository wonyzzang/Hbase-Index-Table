package ac.ku.milab.hbaseindex.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QueryProcess {

	private static byte[] tableName = Bytes.toBytes("test");
	private byte[] family = Bytes.toBytes("cf1");
	private byte[] qualCarNum = Bytes.toBytes("car_num");
	private byte[] qualTime = Bytes.toBytes("time");
	private static byte[] qualLat = Bytes.toBytes("lat");
	private static byte[] qualLon = Bytes.toBytes("lon");

	static class QueryMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable> {
		@Override
		public void map(ImmutableBytesWritable key, Result values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				double k = 0;
				for (Cell c : values.listCells()) {
					if (Bytes.equals(CellUtil.cloneQualifier(c), qualLat)) {
						byte[] lat = CellUtil.cloneValue(c);
						double dLat = Bytes.toDouble(lat);
						k = k+dLat;
					}else if(Bytes.equals(CellUtil.cloneQualifier(c), qualLon)){
						byte[] lon = CellUtil.cloneValue(c);
						double dLon = Bytes.toDouble(lon);
						k = k+dLon;
					}
				}
				context.write(key, new DoubleWritable(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class QueryReducer extends Reducer<ImmutableBytesWritable, DoubleWritable, Text, DoubleWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			// TODO Auto-generated method stub
			for (DoubleWritable a : values) {
				sum += a.get();
			}
			byte[] a = key.get();
			Text b = new Text(Bytes.toString(a));
			context.write(b, new DoubleWritable(sum));
		}
	}

//	public static void main(String[] args) throws Exception {
//		Configuration conf = HBaseConfiguration.create();
//		Scan scan = new Scan();
//		//Filter f = new CarNumFilter(Bytes.toBytes("13오7911"));
//		//Filter f1 = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("137911")));
//		//scan.setFilter(f);
//		
//
//		Job job = new Job(conf, "import file");
//		job.setJarByClass(QueryProcess.class);
//		TableMapReduceUtil.initTableMapperJob(tableName, scan, QueryMapper.class, ImmutableBytesWritable.class, DoubleWritable.class,
//				job);
//		job.setReducerClass(QueryReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(DoubleWritable.class);
//		job.setNumReduceTasks(1);
//		FileOutputFormat.setOutputPath(job, new Path("result"));
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}
}
