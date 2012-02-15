package com.example.wordcount;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text,
        LongWritable>{

  /**
   * Called once for each key/value pair in the input split. Most applications
   * should override this, but the default is the identity function.
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws
          IOException, InterruptedException {
     String line = (String)value.toString();
    String[] words = line.split(" ");
    for(String word: words) {
      context.write(new Text(word), new LongWritable(1));
    }
  }
}
