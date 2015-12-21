package com.hyf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 分组的代码
 * 
 * 解题思路： Map阶段，输入行中姓名作为key，科目和分数作为value，用一个分隔符分开即：zhang3，ma：60。 Reduce阶段，key中就有了
 * <ma:60,ch:78>，这样名字直接作为key输出，而value中各个科目和分数可以在迭代中拼接字符串完成，再定义一个sum变量将分数相加，reduce输出即可。
 * 
数据：test.txt
Zhang3,ma,60
Zhang3,ch,78
Li4,ma,50
Li4,ch,18
处理完成效果
out.txt
Zhang3,60,78,138
Li4,50,18,68
 * @author HuangYongFeng
 *
 */
public class DataGrouping
{
	public static class DataGroupingMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text res = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split(",");
			word.set(itr[0]);
			res.set(itr[1] + ":" + itr[2]);
			context.write(word, res);
		}
	}

	public static class DataGroupingReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			StringBuffer sb = new StringBuffer();
			for (Text val : values)
			{
				String s = val.toString().split(":")[1];
				sum += Integer.parseInt(s);
				sb.append(s + ",");
			}
			sb.append(sum);
			result.set(sb.toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://192.168.1.20:9000/test1/test.txt", "hdfs://192.168.1.20:9000/out6/" };
		Job job = new Job(conf, "DataGrouping"); // 设置一个用户定义的job名称
		job.setJarByClass(DataGrouping.class);
		job.setMapperClass(DataGroupingMapper.class); // 为job设置Mapper类
		job.setReducerClass(DataGroupingReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
