package com.jisuan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CourseOne {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CourseOne.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定本次mr mapper阶段输出  k v 类型
       /* job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);*/


        FileInputFormat.setInputPaths(job, new Path("D:\\chuli\\input\\score.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\chuli\\output"));

        boolean isDone = job.waitForCompletion(true);
        if (isDone){
            System.out.println("成功");
        }else{
            System.out.println("失败");
        }

    }
}
