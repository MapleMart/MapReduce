package com.hyf.mapreduce;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Top K的代码
 * 
 * 解题思路： Map阶段，将输入的行同时作为k，v放入一个TreeMap中，利用了TreeMap的排序，每次加入数据都判断是否超出了treemap的上限，如果超过了就remove掉前面的。
 * 然后Map函数都执行完后根据map类的流程会默认执行一个cleanup函数，在这个函数中可以将treemap中的内容写入map的输出中，其中key可以任意一个字符串，value是数据。
 * 然后进入reduce中通过同样的方法将每个map的topk合并成一个完整的topk，由于所有的键都是一样的，所以都在一个迭代器中完成，因此直接将所有的value放入treemap后迭代输出到hdfs中即可。
 * 
数据：test.txt
1
2
1
4
7
1
处理完成效果
out.txt
7
4
2
 * @author HuangYongFeng
 *
 */
public class TopK
{
	public static class TopKMapper extends Mapper<Object, Text, NullWritable, LongWritable>
	{
		public static final int K = 3;
		private TreeMap<Long, Long> tm = new TreeMap<Long, Long>();

		protected void map(Object key, Text value, Context context) throws IOException
		{
			try
			{
				long k = Integer.parseInt(value.toString());
				tm.put(k, k);
				if (tm.size() > K)
				{
					tm.remove(tm.firstKey());
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (Long text : tm.values())
			{
				context.write(NullWritable.get(), new LongWritable(text));
			}
		}
	}

	public static class TopKReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable>
	{
		public static final int K = 3;
		private TreeMap<Long, Long> mt = new TreeMap<Long, Long>();

		protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			for (LongWritable value : values)
			{
				mt.put(value.get(), value.get());
				if (mt.size() > K)
				{
					mt.remove(mt.firstKey());
				}
			}
			for (Long val : mt.descendingKeySet())
			{
				context.write(NullWritable.get(), new LongWritable(val));
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://192.168.1.20:9000/test1/test.txt", "hdfs://192.168.1.20:9000/out3/" };
		Job job = new Job(conf, "TopK"); // 设置一个用户定义的job名称
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class); // 为job设置Mapper类
		job.setReducerClass(TopKReducer.class); // 为job设置Reducer类
		job.setNumReduceTasks(1);// 设置reduce个数
		job.setOutputKeyClass(NullWritable.class); // 为Map的输出数据设置Key类
		job.setOutputValueClass(LongWritable.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
