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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class GroupPatients extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(GroupPatients.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new GroupPatients(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "grouppatients");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
   // private final static IntWritable one = new IntWritable(1);
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile(",");   
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {

      String[] line = WORD_BOUNDARY.split(lineText.toString());
	 String s2 = line[1];
	 String s4 = line[3];

        if (!(s2.equals("gender"))){
            if (s2 != null && Integer.parseInt(s2) == 1){
                         if (s4.equals("NULL") || s4.equals(null)){
                         context.write(new Text(s2),new DoubleWritable(0.0));
			} else {
                         double height = Double.parseDouble(s4);
                         context.write(new Text(s2),new DoubleWritable(height));
                   }
		}
            if (s2 != null && Integer.parseInt(s2) == 2){
                         if (s4.equals("NULL") || s4.equals(null)){
                         context.write(new Text(s2),new DoubleWritable(0.0));
			} else {
                         double height = Double.parseDouble(s4);
                         context.write(new Text(s2),new DoubleWritable(height));
                   }
		}
          }
    }
  }

  public static class Reduce extends Reducer<Text,DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    public void reduce(Text gender, Iterable<DoubleWritable> Vals, Context context)
        throws IOException, InterruptedException {
      int totalGender = 0;
      double maxVal = 0.0;
 
      for (DoubleWritable val : Vals) {
         totalGender = totalGender + 1;
        if (val.get() > maxVal){
         maxVal = val.get();
         }
      }
      context.write(new IntWritable(totalGender), new DoubleWritable (maxVal));
    }
  }
}
