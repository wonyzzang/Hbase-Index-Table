package ac.ku.milab.hbaseindex.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// for Practice
public class ImportFromFile {
	
	public static final String NAME = "ImportFromFile";
	public enum Counters {LINES}
	
	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		private Random rand = new Random();
		private byte[] tableName = null;
		private byte[] family = null;
		private byte[] qualifier = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			String column = context.getConfiguration().get("conf.column");
			String table = context.getConfiguration().get("conf.tableName");
			tableName = Bytes.toBytes(table);
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			family = colkey[0];
			if(colkey.length>1){
				qualifier = colkey[1];
			}
		}
		
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException{
			try{
				String lineString = line.toString();
				byte[] rowkey = Bytes.toBytes(rand.nextInt(10000));
				Put put = new Put(rowkey);
				put.add(family, qualifier, rowkey);
				context.write(new ImmutableBytesWritable(tableName), put);
				
				context.getCounter(Counters.LINES).increment(1);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
	}
	
	private static CommandLine parseArgs(String[] args) throws ParseException{
		Options options = new Options();
		Option o = new Option("t", "table", true, "table to import into (must exist}");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("c", "column", true, "column to store row data into (must exist)");
		o.setArgName("family:qualifier");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("i", "input",true, "the directory or file to read from");
		o.setArgName("path-in-HDFS");
		o.setRequired(true);
		options.addOption(o);
		options.addOption("d", "debug", false, "switch on DEBUG log level");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		
		try{
			cmd = parser.parse(options, args);
		}catch(Exception e){}
		
		return cmd;
	}
	/*
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String table = cmd.getOptionValue("t");
		String input = cmd.getOptionValue("i");
		String column = cmd.getOptionValue("c");
		conf.set("conf.column", column);
		conf.set("conf.tableName", table);
		Job job = new Job(conf, "import file");
		job.setJarByClass(ImportFromFile.class);
		job.setMapperClass(ImportMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		//job.setOutputKeyClass(ImmutableBytesWritable.class);
		//job.setOutputValueClass(Put.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/
}
