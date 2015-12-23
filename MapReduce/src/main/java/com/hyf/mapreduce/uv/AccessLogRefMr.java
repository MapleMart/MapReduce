package com.hyf.mapreduce.uv;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hyf.mapreduce.utils.AnalysisNginxTool;
import com.hyf.mapreduce.utils.DateToNUM;

/**
 * 网站每日每个连接来源统计: 解题思路：
 * Map：从输入的行中取出访问日期（从访问时间里面处理出来，注意为了做到排序输出此处必须得使用时间戳格式，而不能用传统的format格式）以及请求url地址（
 * 去除host部分）作为key，来源url作为value。 这样Map的输出就是：日期_url，集合<url1，url1，url2，…………>
 * Reduce：迭代value中的url放入hashmap中，首先判断是否有url如果没有就放入url，1，如果有就将hashmap对应的值+1，最后迭代打印。
 * @author 黄永丰
 * @createtime 2015年12月23日
 * @version 1.0
 */
public class AccessLogRefMr
{
	public static class AccessLogRefMrMapper extends Mapper<Object, Text, Text, Text>
	{
		private final static Text urlr = new Text("");
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split(" ");
			if (itr.length < 11)
			{
				return;
			}
			String date = AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
			String url = itr[6];
			String urlref = itr[10];
			word.set(date + "_" + url);
			urlr.set(urlref);
			context.write(word, urlr);
		}
	}

	public static class AccessLogRefMrReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Map<String, Integer> urlr = new HashMap<String, Integer>();
			for (Text val : values)
			{
				String urlref = val.toString();
				Integer i = urlr.get(urlref);
				if (i != null)
				{
					i++;
					urlr.put(urlref, i);
				}
				else
				{
					urlr.put(urlref, 1);
				}
			}
			for (String m : urlr.keySet())
			{
				result.set(m + "\t" + urlr.get(m));
				context.write(key, result);
			}

		}
	}

	public static void main(String[] args) throws Exception
	{
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/data/access.log", "hdfs://master:9000/uvout/AccessLogRefMr" };
		Job job = new Job(conf, "ref"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogRefMr.class);
		job.setMapperClass(AccessLogRefMrMapper.class); // 为job设置Mapper类
		job.setReducerClass(AccessLogRefMrReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
