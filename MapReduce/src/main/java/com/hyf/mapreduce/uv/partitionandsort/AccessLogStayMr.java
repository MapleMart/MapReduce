package com.hyf.mapreduce.uv.partitionandsort;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hyf.mapreduce.utils.AnalysisNginxTool;
import com.hyf.mapreduce.utils.DateToNUM;

/**
 * 代码实现网站每日用户功能驻留统计: 
 * 解题思路： 首先日志文件中不仅仅有一个用户的连接点击信息，如果想算出页面的驻留时间，首先就得需要对文件进行按照用户ip_时间戳排序，并且为了得到
 * 用户进入和跳出连接的中间值，我们就必须得到进入时间和跳出时间差，进入时间其实就是一条日志的生成时间，跳出时间其实也不难发现，只要按照用户操作时间
 * 排序后的日志，紧随的那个日志条目就是用户离开本页进入下一页的时间，因此我们首先要通过mapreduce处理出一个这样格式的日志文本： ip_时间戳，url1|=end=|ip_时间戳，url2
 * ip_时间戳，url2|=end=|ip_时间戳，url3 为了分隔符与日志内容不重复，我们使用一个复杂的不可能出现的文字串作为分隔符|=end=|
 * 然后第二个mapreduce就可以处理第一个mapreduce处理好的这个结果，得到所有的日期_url对应的时间间隔，在reduce中把时间间隔相加
 * @author 黄永丰
 * @createtime 2015年12月23日
 * @version 1.0
 */
public class AccessLogStayMr
{
	public static class AccessLogStayMrMapper extends Mapper<Object, Text, Text, Text>
	{
		private final static Text k = new Text("");
		private Text v = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split(" ");
			if (itr.length < 7)
			{
				return;
			}
			long date = AnalysisNginxTool.nginxDateStmpToDateTime(itr[3]);
			String url = itr[6];
			k.set(itr[0] + "|" + date);
			v.set(url);
			context.write(k, v);

		}
	}

	public static class FirstPartitioner extends Partitioner<Text, Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{
			String k = key.toString().split("\\|")[0];
			return k.hashCode() % numPartitions;
		}
	}

	public static class GroupingComparator extends WritableComparator
	{
		protected GroupingComparator()
		{
			super(Text.class, true);
		}

		@Override
		// Compare two WritableComparables.
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			Text a = (Text) w1;
			Text b = (Text) w2;
			int l = a.toString().split("\\|")[0].hashCode();
			int r = b.toString().split("\\|")[0].hashCode();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public static class AccessLogStayMrReducer extends Reducer<Text, Text, Text, NullWritable>
	{
		private Text k = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String urltmp = "";
			for (Text val : values)
			{
				System.out.println(urltmp);
				k.set(urltmp + "|=end=|" + key.toString() + "|" + val.toString());
				if (!urltmp.equals(""))
				{
					context.write(k, NullWritable.get());
				}
				urltmp = key.toString() + "|" + val.toString();
			}
		}
	}

	public static class AccessLogStayMrMapper2 extends Mapper<Object, Text, Text, LongWritable>
	{
		private final static Text k = new Text("");
		private LongWritable v = new LongWritable(0);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split("\\|=end=\\|");
			if (itr.length != 2)
			{
				return;
			}
			String str1[] = itr[0].split("\\|");
			String str2[] = itr[1].split("\\|");

			String ip = str1[0];
			long datetime1 = Long.parseLong(str1[1]);
			String url = str1[2];
			long datetime2 = Long.parseLong(str2[1]);
			Date d = new Date(datetime1);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
			System.out.println(sdf1.format(d));
			k.set(ip + "|" + url + "|" + sdf.format(d));
			v.set(datetime2 - datetime1);
			context.write(k, v);
			// System.out.println(k.toString()+"===="+v.toString());
		}
	}

	public static class AccessLogStayMrReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable result = new LongWritable(0);

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for (LongWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/data/access.log", "hdfs://master:9000/uvout/umtmp", "hdfs://master:9000/uvout/AccessLogStayMr" };
		Job job = new Job(conf, "stay"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogStayMr.class);
		job.setMapperClass(AccessLogStayMrMapper.class); // 为job设置Mapper类
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(AccessLogStayMrReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		job.waitForCompletion(true); // 运行job

		Job job2 = new Job(conf, "stay2"); // 设置一个用户定义的job名称
		job2.setJarByClass(AccessLogStayMr.class);
		job2.setMapperClass(AccessLogStayMrMapper2.class); // 为job设置Mapper类
		job2.setReducerClass(AccessLogStayMrReducer2.class); // 为job设置Reducer类
		job2.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job2.setOutputValueClass(LongWritable.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));// 为job设置输出路径
		System.exit(job2.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
