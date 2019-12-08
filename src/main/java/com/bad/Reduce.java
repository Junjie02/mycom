package com.bad;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reduce extends org.apache.hadoop.mapreduce.Reducer {
    public void reduce (IntWritable key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
        int sum=0;
        for (Text val :
                values) {
             String[] arr= val.toString().split("\t");
             sum += Integer.parseInt(arr[2]);
            context.write(key, val);
        }
       // context.write(new Text("总商品总浏览次数"),sum);
    }
}
