package com.example.linecount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LineCountReducer extends Reducer<Text, LongWritable, Text,
        LongWritable>{

  long sum = 0;

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context
          context) throws IOException, InterruptedException {
    for (LongWritable value : values) {
       sum = sum + (new Long(value.toString())).longValue();
     }
  }

  @Override
  protected void cleanup(Context context) throws IOException,
          InterruptedException {
    context.write(new Text("total-linecount"), new LongWritable(sum));

  }

}
