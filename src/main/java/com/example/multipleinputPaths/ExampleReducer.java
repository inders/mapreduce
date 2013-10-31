package com.example.multipleinputPaths;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 31/10/13
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExampleReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String details  = "";
        for (Text twitterDetails : values) {
            details += twitterDetails.toString()  + ",";
        }
        details += "\n";
        context.write(key, new Text(details));
    }
}
