package com.had;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 这个类就是mr程序运行时主类，本类中组装了一些程序运行时所需的信息
 * 比如：使用的那个mapper类 那个类reducer类 输入数据在那 输出数据在什么地方
 */

public class WordCountDriver {
    public static void main(String[] args)throws Exception {
        //遍历Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定mr所用的mapper reducer类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次mr mapper阶段输出  k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定本次mr 最终输出k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定本次mr输入输入的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交程序并提交打印
        boolean b = job.waitForCompletion(true);
        if(b){
            System.out.println("程序运行成功");
        }else{
            System.out.println("程序运行失败");
        }
    }
}
