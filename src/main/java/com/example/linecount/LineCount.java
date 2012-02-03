package com.example.linecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LineCount extends Configured implements Tool{
  @Override
  public int run(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(args[0]));
    Path inputPath = new Path("input-distlinecount");
    FSDataOutputStream fout= fs.create(inputPath);
    for(FileStatus fileStatus : fileStatuses) {
      fout.writeBytes(fileStatus.getPath().makeQualified(fs).toString());
      fout.writeBytes("\n");
    }
    fout.close();

    Job job = new Job(new Configuration());
    FileInputFormat.addInputPath(job, inputPath) ;
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("distributed-linecount");
    job.setJarByClass(LineCount.class);
    job.setMapperClass(LineCountMapper.class);
    job.setReducerClass(LineCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.waitForCompletion(true);
    if(job.isSuccessful()) {
      System.out.println("job is success");

    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new LineCount(), args );
  }
}
