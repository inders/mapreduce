package com.example.setdifference;


import javax.print.attribute.standard.JobName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Here we use MAPREDUCE to do a set operation A-B
 * Highlights ->
 * Mapper1 uses FileSplit from context to get FileName a split belongs to
 * setA, setB file names are passed to map and reduce using configuration
 * custom properties
 *
 * Approach
 * 1. each split will be run by a different mapper
 * 2. Mapper1 Output -> key = line/word, value = fileName
 * 3. ExampleReducer Check -> if a key contains setB has value don't emit in final
 * output
 */

public class SetDifferenceDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("SET-A", "fileA");
        configuration.set("SET-B", "fileB");

        Job job = new Job(configuration, "set-difference");

        FileInputFormat.addInputPath(job,
                new Path("src/main/resources/set-difference"));

        FileOutputFormat.setOutputPath(job, new Path("/tmp/set-difference"));

        job.setMapperClass(SetDifferenceMapper.class);
        job.setReducerClass(SetDifferenceReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true)?0 : -1;
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new SetDifferenceDriver(), args);
    }


}
