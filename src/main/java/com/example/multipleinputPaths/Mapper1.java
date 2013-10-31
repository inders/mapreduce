package com.example.multipleinputPaths;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 31/10/13
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class Mapper1 extends Mapper<Text, Text, Text, Text> {

    //identity mapper
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Text tweetID = key;
        Text tweetMessage = value;
        context.write(key, value);

    }
}
