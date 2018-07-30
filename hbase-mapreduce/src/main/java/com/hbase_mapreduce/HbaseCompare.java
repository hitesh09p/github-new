package com.hbase_mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
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
		List<Scan> scans = new ArrayList<Scan>();

		Scan scan1 = new Scan();
		scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes("inception_tbl1"));
		System.out.println(scan1.getAttribute("scan.attributes.table.name"));
		scans.add(scan1);

		Scan scan2 = new Scan();
		scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes("fin_str1"));
		System.out.println(scan2.getAttribute("scan.attributes.table.name"));
		scans.add(scan2);

		TableMapReduceUtil.initTableMapperJob(       
			scans,               // Scan instance to control CF and attribute selection
			MyMapper.class,     // mapper class
			Text.class,         // mapper output key
			Text.class,  // mapper output value
			job);
		//job.setReducerClass(MyReducer.class);    // reducer class
	//	job.setNumReduceTasks(1);    // at least one, adjust as required
		
		FileOutputFormat.setOutputPath(job, new Path("/newmroutput")); // adjust directories as required
        
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
	public static class MyMapper extends TableMapper<Text, Text>  {
		private static byte[] inception_tbl1 = Bytes.toBytes("inception_tbl1");
		private static byte[] fin_str1 = Bytes.toBytes("fin_str1");
		
		byte[] acc_key;
		String Acc_key;
		// private Text col_name = new Text("acc-key");
		 Text mapperKey;
		 Text mapperValue;
		 

		  public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
			  
			  TableSplit currentSplit = (TableSplit)context.getInputSplit();
				byte[] tableName = currentSplit.getTableName();
			  
				try {
					if (Arrays.equals(tableName, inception_tbl1)) {
						acc_key = columns.getValue(Bytes.toBytes("acc"), Bytes.toBytes("acc_key"));
						Acc_key = new String(acc_key);
						
						mapperKey = new Text("acc_key_tbl1");
						mapperValue = new Text(Acc_key);
						context.write(mapperKey, mapperValue);
					} else if (Arrays.equals(tableName, fin_str1)) {
						acc_key = columns.getValue(Bytes.toBytes("acc"), Bytes.toBytes("acc_key"));
						Acc_key = new String(acc_key);
						
						mapperKey = new Text("acc_key_tbl2");
						mapperValue = new Text(Acc_key);
						context.write(mapperKey, mapperValue);
					}
				} catch (Exception e) {
					// TODO : exception handling logic
					e.printStackTrace();
				}
			  
			  
		
		  }
		}
	
	/*public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		    int i = 0;
		    for (IntWritable val : values) {
		      i += val.get();
		    }
		    context.write(key, new IntWritable(i));
		  }
		}*/
		

	

}
