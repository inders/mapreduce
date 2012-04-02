package com.example.setdifference;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SetDifferenceMapper extends Mapper<LongWritable, Text,
Text, Text>{
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    Text fileName = new Text(fileSplit.getPath().getName());
    //using NLineInputFormat -> key -> offset, value -> line
    //output is line, fileName
    context.write(value, fileName);
  }
}
