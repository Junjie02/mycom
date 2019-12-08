package com.bad;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<Object, Text, IntWritable, Text> {
    private static Text goods =new Text();
    private static IntWritable num = new IntWritable();
    /*
    *   首先将文件序列化
    * 序列化完成，以\t为分隔符进行切割
    * 存入数组以商品点击数为key 就是num，以商品序号为value goods
    *
     */
    public void  map(Object key,Text value,Context context)throws IOException,InterruptedException{
        String line = value.toString();

        String[] arr = line.split("\t");

        num.set(Integer.parseInt(arr[2]));//封装
        goods.set(arr[1]);
        context.write(num,goods);//写入<k,v>
    }
}
