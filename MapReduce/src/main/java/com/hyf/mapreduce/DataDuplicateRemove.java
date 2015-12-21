package com.hyf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce操作:(数据去重)
 *	创建test1目录并在此目录创建test.txt文件(
 *		用HDFS创建好下面数据,用命令( 循环列出目录、子目录及文件信息：
 *								[root@master local]# hadoop fs -ls -R /
 *								[root@master local]# hadoop fs -mkdir /test1
 *							    [root@master local]# vi test.txt (写入数据如下)
 *							    [root@master local]# hadoop fs -put test.txt /test1)
 *		或用代码(HDFS工程的MyHDFS类)都可以)
数据：test.txt
1
2
1
4
7
1
处理完成效果
out.txt
1
2
4
7
 * 在hadoop2.7.1解压出来,hadoop1.x.x的话是不一样的(Hadoop1.x.x跟目录下的jar包和lib目录下的jar包) 
 * 在hadoop-2.7.1\share\hadoop\common 下导入以下包：
 *  --hadoop-common-2.7.1.jar --hadoop-common-2.7.1-tests.jar
 * 	--hadoop-nfs-2.7.1.jar 
 * 在hadoop-2.7.1\share\hadoop\common\lib 下导入所有的包(63个) 
 * 	--... 
 * 在hadoop-2.7.1\share\hadoop\mapreduce 下导入所有的包(9个)
 *  --..
 * 在hadoop-2.7.1\share\hadoop\hdfs 下导入以下包
 *  --hadoop-hdfs-2.7.1.jar
 * 	--hadoop-hdfs-2.7.1-tests.jar
 *  --hadoop-hdfs-nfs-2.7.1.jar 
 * 在hadoop-2.7.1\share\hadoop\yarn 下导入以下包
 *  --hadoop-yarn-common-2.7.1.jar
 * 	--hadoop-yarn-api-2.7.1.jar
 *  --
 * )
 * @author HuangYongFeng
 */
public class DataDuplicateRemove 
{

	public static class DataDuplicateRemoveMapper extends Mapper<Object, Text, Text, NullWritable>
	{
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++)
			{
				word.set(itr[i]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class DataDuplicateRemoveReducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception
	{
//		File workaround = new File("C:/software/hadoop-2.6.0"); 
//		System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath()); //这两可以加上可以不加,不然找不到hadoop
		
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://192.168.1.20:9000/test1/test.txt", "hdfs://192.168.1.20:9000/out2/" };
		Job job = new Job(conf, "DataDuplicateRemove"); // 设置一个用户定义的job名称
		job.setJarByClass(DataDuplicateRemove.class);
		job.setMapperClass(DataDuplicateRemoveMapper.class); // 为job设置Mapper类
		job.setCombinerClass(DataDuplicateRemoveReducer.class); // 为job设置Combiner类
		job.setReducerClass(DataDuplicateRemoveReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job.setOutputValueClass(NullWritable.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
