package com.example.linecount;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCountMapper<k,v> extends Mapper<LongWritable, Text, Text,
        LongWritable> {

  public void map(LongWritable key, Text value, Context context){
    try {
    // using fileinput format so key is line number
    String fileName = value.toString();
    FileSystem fs;
    fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream fin = fs.open(new Path(fileName));
    long linecount=0;
    while(true) {
      if (fin.readLine() != null)
        linecount++;
      else
        break;
    }
    context.write(value, new LongWritable(linecount));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }


}
