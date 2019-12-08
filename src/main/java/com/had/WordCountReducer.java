package com.had;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 这里是mr程序reducer阶段处理的类
 *
 * KEYIN:就是reducer阶段输入的数据key类型，对应mapper的输出key类型 在本案例中 就是单词 text
 * VALUEIN： 就是reducer阶段输入的数据value类型对mapper的输出value类型 在本案例中 就是单词次数
 * KEYOUT:就是reducer阶段输出的数据key类型 在本案例中 就是单词 Text
 * VALUEOUTreducer阶段输出的数据value类型 在本案例中 就是单词的总次数 IntWritable
 */

public class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    /**
     * 这里是reduce阶段具体业务类的实现方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * reduce接受来自map阶段处理的数据之后，按照key的字典序进行排序
     *
     * 按照key是否相容作为一组去调用reduce方法
     * 本方法的key就是这一组相同kv对的共同key
     *  把这一组所有的v作为一个迭代器传入我们的reduce方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;
        //遍历一组迭代器，把每个单词的1累加起来就构成了单词的总次数
        for(IntWritable value:values){
            count += value.get();
        }
        //把最终的结果输入
        context.write(key, new IntWritable(count));
    }
}
