package com.jisuan;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计每门课程参考人数和课程平均分
 * computer,huangxiaoming,85,86,41,75,93,42,85
 */
public  class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        //对数据进行解析，分析数据可知第三个字段的学生在某门课程中的考试次数
        //根据问题可以分析，统计参考人数，只有使用课程作为key，在reduce阶段最终及数据条数即可
        //对于课程的平均分要统计该课程所有学生的考试次数，以及总分
        //在mapper阶段，统计每一个学生全部的总考试次数和总分
        String[] line = value.toString().split(",");
        //sum用来统计学生在某门课程中的考试成绩
        long sum = 0L;
        //totalTime用来统计学生在某门课程中的考试次数
        //computer,huangxiaoming,85,86,75,93,43,90,80
        //首先数据时通过‘，’ 进行分隔符的，所以通过mapper逐行读取数据然后根据‘，’ 进行切割分得到一个数组
        //然后从第三个元素开始就是某位学生在某门课程中一次考试的成绩
        //所以用数组长度来减去2就是读学生该课程中的总考试次数
        long totalTimes = line.length-2;
        //通过循环遍历累加该学生在该课程中的考试成绩
        for (int i = 2; i < line.length; i++) {
            sum += Long.parseLong(line[i]);
        }
        //最后输出，使用课程名作为key 例如：computer
        //使用拼接字符串的形式创建value，方便reducer阶段的处理
        //使用totalTimes+”——“+sum 这样大方式
        //考试次数 + 总成绩
        context.write(new Text(line[0]), new Text(totalTimes+","+sum));
    }
}
