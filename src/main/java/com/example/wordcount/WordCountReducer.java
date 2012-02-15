package com.example.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text,
        LongWritable>{
  @Override
  protected void reduce(Text word, Iterable<LongWritable> wordCounts, Context
          context) throws IOException, InterruptedException {
    long wordCount = 0;
       //input key val - word, it's count
    for(LongWritable value: wordCounts) {
           wordCount += value.get();
    }
    context.write(word, new LongWritable(wordCount));
  }
}
