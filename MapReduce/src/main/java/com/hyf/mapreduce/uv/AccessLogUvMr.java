package com.hyf.mapreduce.uv;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hyf.mapreduce.utils.AnalysisNginxTool;
import com.hyf.mapreduce.utils.DateToNUM;
import com.hyf.mapreduce.utils.HDFSUtil;

/**
 * 将$HADOOP_HOME下的jar包以及lib下的jar包导入工程中
 * 实现网站连续几日uv量统计: 解题思路：
 * Map：从输入的行中取出访问日期（从访问时间里面处理出来，注意为了做到排序输出此处必须得使用时间戳格式，而不能用传统的format格式）和访问地址拼接作为key，并取出用户ip作为value
 * 这样Map的输出就是：日期_地址，集合<ip1，ip2，…………> Reduce：将key下的value去重，最简单的办法就是取出来放入一个set中统计个数量，然后输出。
 * @author 黄永丰
 * @createtime 2015年12月22日
 * @version 1.0
 */
public class AccessLogUvMr
{
	public static class AccessLogUvMrMapper extends Mapper<Object, Text, Text, Text>
	{
		private final static Text ip = new Text("");
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split(" ");
			if (itr.length < 7)
			{
				return;
			}
			String date = AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
			String url = itr[6];
			word.set(date + "_" + url);
			ip.set(itr[0]);
			context.write(word, ip);
		}
	}

	public static class AccessLogUvMrReducer extends Reducer<Text, Text, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Set<String> ipset = new HashSet<String>();
			for (Text val : values)
			{
				ipset.add(val.toString());
			}
			result.set(ipset.size());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		//创建hadoop里的hdfs的文件夹
		HDFSUtil.mkdirsFile("hdfs://master:9000/data");
		//把nginx的日志access.log文件上传到hadoop的文件系统里
		HDFSUtil.copyFromLocalFile("/Users/huangyongfeng/Documents/learning-resources/hadoop/access.log", "hdfs://master:9000/data");
		
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		//用HDFS把access.log文件放到hdfs://master:9000/data目录下
		String[] otherArgs = { "hdfs://master:9000/data/access.log", "hdfs://master:9000/uvout" };
		Job job = new Job(conf, "uv"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogUvMr.class);
		job.setMapperClass(AccessLogUvMrMapper.class); // 为job设置Mapper类
		job.setReducerClass(AccessLogUvMrReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
