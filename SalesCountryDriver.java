package SalesCountry;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalesCountryDriver extends Configured implements Tool {

    public static final String OTHER = "OTHER";
    public static final String MUMBAI = "MUMBAI";
    public static final String DELHI = "DELHI";
    public static final String AHMEDABAD = "AHMEDABAD";
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new SalesCountryDriver(), args);
        System.exit(exitCode);
    }
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Please provid two arguments :");
            System.out.println("[ 1 ] Input dir path");
            System.out.println("[ 2 ] Output dir path");
            return -1;
        }
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Job job=Job.getInstance();
        job.setJarByClass(SalesCountryDriver.class);
        job.setMapperClass(SalesCountry.SalesMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class); //Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); // Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        MultipleOutputs.addNamedOutput(job,"AHMEDABAD", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"DELHI", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"MUMBAI", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"OTHER", TextOutputFormat.class,Text.class,Text.class);
        boolean success = job.waitForCompletion(true);
        return (success?0:1);
    }
}

/*
private class SalesCountryDriverOld {
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SalesCountryDriver.class);

        // Set a name of the Job
        job_conf.setJobName("SalePerCountry");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(SalesCountry.SalesMapper.class);
        job_conf.setReducerClass(SalesCountry.SalesCountryReducer.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments, 
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

        my_client.setConf(job_conf);
        try {
            // Run the job 
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
*/
