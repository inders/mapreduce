package com.example.linecount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;

public class LineCountReducer extends Reducer<Text, LongWritable, Text,
        LongWritable>{

  public void reduce(Text key, Iterator<LongWritable> values,
                     Context context) throws  Exception{
    long sum = 0;
    while (values.hasNext()) {
      sum = sum + (new Long(values.next().toString())).longValue();
     context.write(key, new LongWritable(sum));
    }

  }

}
