package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.*;
import com.google.common.collect.Iterables;

import org.apache.log4j.Logger;

public class PartB extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PartB.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new PartB(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "partB");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private long numRecords = 0;    
	    public void map(LongWritable offset, Text value, Context context)
	        throws IOException, InterruptedException {
	    // Split the data into two, using the comma as the demarcator
	    String data[] = value.toString().split(",");
	    // Assign veriables to each index of the splitted data
        String word1 = data[0];
	    String word2 = data[1];
	    // Write a value of One (1) for all the data on first index
	    context.write(new Text(word1), new IntWritable(1));
	    // Write a value of Zeros (1) for all the data on first index
	    context.write(new Text(word2), new IntWritable(0));
	      
	    }
	  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
	    private Text word = new Text();
	    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
	        throws IOException, InterruptedException {
	      // Create a counter sum
	      int sum = 0;
	      // Get all the unique key value sum
	      for (IntWritable count : counts) {
	        sum += count.get();
	      }
	      // If the key value sum is equal to zero, they definitely, it did not appear on the first column, 
	      // then we write out the key.
	        if (sum == 0){
	        	context.write(word, NullWritable.get());
	        }else {
	        	// Else do nothing
	        } 
	    }
	  }
}
