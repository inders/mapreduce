package com.example.setdifference;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SetDifferenceReducer extends Reducer<Text, Text, Text, Text>{
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   // key -> SET VALUE
   // value -> FILE's where this value occurs
   String setAFile = context.getConfiguration().get("SET-A");
    String setBFile = context.getConfiguration().get("SET-B");
    for (Text value: values) {
      if (value.toString().equalsIgnoreCase(setBFile))
        return;
    }
    context.write(key, null);

  }
}
