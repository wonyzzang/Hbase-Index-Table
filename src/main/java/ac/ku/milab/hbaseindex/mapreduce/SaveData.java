package ac.ku.milab.hbaseindex.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SaveData {

	public SaveData() throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf, "import file");
		job.setJarByClass(SaveData.class);
		job.setMapperClass(SaveDataMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("example_data"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		SaveData td = new SaveData();
	}

	static class DataParser {
		private String carNum = null;
		private long time = 0;
		private double lat = 0L;
		private double lon = 0L;

		public DataParser(Text text) {
			String[] token = text.toString().split(",");

			carNum = token[0];
			time = Long.parseLong(token[1]);
			lat = Double.parseDouble(token[2]);
			lon = Double.parseDouble(token[3]);
		}

		public String getCarNum() {
			return carNum;
		}

		public long getTime() {
			return time;
		}

		public double getLatitude() {
			return lat;
		}

		public double getLongitude() {
			return lon;
		}
	}

	static class SaveDataMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private byte[] tableName = Bytes.toBytes("test");
		private byte[] family = Bytes.toBytes("cf1");
		private byte[] qualCarNum = Bytes.toBytes("car_num");
		private byte[] qualTime = Bytes.toBytes("time");
		private byte[] qualLat = Bytes.toBytes("lat");
		private byte[] qualLon = Bytes.toBytes("lon");

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException {

			try {
				DataParser dp = new DataParser(line);
				String carNum = dp.getCarNum();
				long time = dp.getTime();
				double lat = dp.getLatitude();
				double lon = dp.getLongitude();

				byte[] rowkey = Bytes.add(Bytes.toBytes(carNum + "_"), Bytes.toBytes(time));

				Put put = new Put(rowkey);
				put.addColumn(family, qualCarNum, Bytes.toBytes(carNum));
				put.addColumn(family, qualTime, Bytes.toBytes(time));
				put.addColumn(family, qualLat, Bytes.toBytes(lat));
				put.addColumn(family, qualLon, Bytes.toBytes(lon));

				context.write(new ImmutableBytesWritable(tableName), put);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
