package com.example.multipleinputPaths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 31/10/13
 * Time: 11:20 AM
 * Demonstrate how to read two different input files with two different types of mappers and then join them on the same key
 * in the reducer
 */
public class Driver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("key.value.separator.in.input.line", ",");


        Job job = new Job(configuration, "multiple-inputs-mapper");

        //TODO: how to set different delimeters for KeyValueTextInputFormat for different Mappers
        MultipleInputs.addInputPath(job, new Path("src/main/resources/multiinput/input1"), KeyValueTextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job, new Path("src/main/resources/multiinput/input2"), KeyValueTextInputFormat.class, Mapper2.class);


        job.setReducerClass(ExampleReducer.class);
        job.setNumReduceTasks(2);
        //TODO: How to set delimeter between key and values in the textinputFormat
        job.setOutputFormatClass(TextOutputFormat.class);

        //set the mapper output types for keys and values as we we have used TextOutputFormat
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("/tmp/multi-input-tweet-join"));

        //
    //    MultipleOutputs.addNamedOutput(job,);

        return job.waitForCompletion(true)?0 : -1;
    }


    public static void main(String[] args) throws Exception{
        ToolRunner.run(new Driver(), args);
    }



}
