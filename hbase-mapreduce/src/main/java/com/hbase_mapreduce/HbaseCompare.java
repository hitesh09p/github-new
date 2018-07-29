package com.hbase_mapreduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HbaseCompare {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration config = HBaseConfiguration.create();
		//conf.addResource("hbase-site.xml");
        //conf.set("hbase.master","ubuntu:60000");
      //  conf.set("hbase.zookeeper.quorum", "zks1,zks2,zks3");
      //  conf.set("hbase.zookeeper.property.clientPort", "2181");
		Job job = new Job(config,"HbaseCompare");
		job.setJarByClass(com.hbase_mapreduce.HbaseCompare.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);
		//scan.setAttribute("scan.attributes.table.name", Bytes.toBytes("inception_tbl"));
		// don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			"inception_tbl1",        // input table
			scan,               // Scan instance to control CF and attribute selection
			MyMapper.class,     // mapper class
			Text.class,         // mapper output key
			IntWritable.class,  // mapper output value
			job);
		job.setReducerClass(MyReducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
		
		FileOutputFormat.setOutputPath(job, new Path("/mr/mySummaryFile")); // adjust directories as required
        
		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static class MyMapper extends TableMapper<Text, IntWritable>  {
		  public static final byte[] CF = "cf".getBytes();
		  public static final byte[] ATTR1 = "attr1".getBytes();

		  private final IntWritable ONE = new IntWritable(1);
		  private Text text = new Text();

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		    String val = new String(value.getValue(CF, ATTR1));
		    text.set(val);     // we can only emit Writables...
		    context.write(text, ONE);
		  }
		}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		    int i = 0;
		    for (IntWritable val : values) {
		      i += val.get();
		    }
		    context.write(key, new IntWritable(i));
		  }
		}
		

	

}
