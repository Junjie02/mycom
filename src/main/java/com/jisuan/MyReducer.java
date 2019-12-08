package com.jisuan;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //相同的课程会被分到一个组
        //考试人数数据
        int count = 0;
        //得分累加器
        int totalScore = 0;
        //考试次数计数器
        int examTemes = 0;
        for (Text t: values){
            String[] arrs = t.toString().split(",");
            count++;
            totalScore += Integer.parseInt(arrs[1]);
            examTemes += Integer.parseInt(arrs[0]);
        }
        //求平均分
        float avg = totalScore*1.0F/examTemes;
        //输出结果
        context.write(key, new Text(count+"\t"+avg));
    }
}
