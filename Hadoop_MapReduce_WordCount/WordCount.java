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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {
  public enum Operations { UNIQUE_WC }
  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
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
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	  // Split the data into two, using the comma as the demarcator
      String data[] = lineText.toString().split(",");
      Text currentWord  = new Text();
      // Assign veriables to each index of the splitted data
    	String word1 = data[0];
    	String word2 = data[1];
    	// Run and If statement to check the order of pairs using Java compareTo Method
    	// Then write the correct order
    		  if (word2.compareTo(word1)<1)
    		  {
    context.write(new Text(word2 + "," + word1), new IntWritable(1));
    
    		  }else  {
    			  context.write(new Text(word1 + "," + word2), new IntWritable(1));
    		  }
    	        currentWord = new Text(word);
    	        context.write(currentWord,one);
    }
  }
  
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
    	 // Create a counter sum
      int sum = 0;
      int unique =0;
      // Get all the unique key value sum
      for (IntWritable count : counts) {
        sum += count.get();
        unique += 1;
      }
      // then we write both key and value to out put
      context.write(word, new IntWritable(sum));
      context.getCounter(WordCount.Operations.UNIQUE_WC).increment(1);
    }
  }
}
