package com.bad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OneSort {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        Job job=new Job(conf,"OneSort");
        job.setJarByClass(OneSort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path("D:\\chuli\\input\\"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\chuli\\output\\"));
        boolean b = job.waitForCompletion(true);
        if(b){
            System.out.println("程序运行成功");
        }else{
            System.out.println("程序运行失败");
        }


    }
}
