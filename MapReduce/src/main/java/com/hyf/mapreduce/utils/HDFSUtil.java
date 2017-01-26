package com.hyf.mapreduce.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS操作:(1、连接到hadoop2.7.1的HDFS把文件上传到hadoop服务器里)
 * 在hadoop2.7.1解压出来,hadoop1.x.x的话是不一样的(Hadoop1.x.x跟目录下的jar包和lib目录下的jar包)
 * 在hadoop-2.7.1\share\hadoop\common 下导入以下包： --hadoop-common-2.7.1.jar
 * --hadoop-common-2.7.1-tests.jar --hadoop-nfs-2.7.1.jar 在hadoop-2.7.1\share\hadoop\common\lib
 * 下导入所有的包(63个) --... 在hadoop-2.7.1\share\hadoop\hdfs 下导入以下包 --hadoop-hdfs-2.7.1.jar
 * --hadoop-hdfs-2.7.1-tests.jar --hadoop-hdfs-nfs-2.7.1.jar )
 * @author HuangYongFeng
 */
public class HDFSUtil
{
	private static Configuration conf = new Configuration();	
	static
	{
		// 1、连接到hadoop2.7.1的HDFS
		conf.set("fs.default.name", "hdfs://master:9000");// 如果不写就只能本地操作了 hdfs://master:9000(也可以这样写,不过要做主机名master映射)
		conf.set("hadoop.job.ugi", "root,root");// 如果不写系统将按照默认的用户进行操作
	}

	/**
	 * 上传文件
	 * @author 黄永丰
	 * @createtime 2015年12月23日
	 * @param dataPath 数据的路径
	 * @param hdfsPath 要上传hadoop的hdfs路径
	 */
	public static void copyFromLocalFile(String dataPath, String hdfsPath)
	{
		FileSystem fs = null;
		try
		{
			fs = FileSystem.get(conf);
			// 2、上传文件到规定文件夹下
			// (第一个参数是本地运行的文件路径,如果在linux运行,那就要按linux系统的路径/usr/local/derby.log,第二个参数是把文件放在hadoop HDFS的路径)
			fs.copyFromLocalFile(new Path(dataPath), new Path(hdfsPath));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				fs.close();
			}
			catch (IOException e1)
			{
				fs = null;
				e1.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @interfaceName
	 * @author 黄永丰
	 * @createtime 2015年12月23日
	 * @param hdfsFilePath 创建在hadoop里的hdfs的文件夹路径
	 */
	public static void mkdirsFile(String hdfsFilePath)
	{
		FileSystem fs = null;
		try
		{
			fs = FileSystem.get(conf);
			// 2、在HDFS中创建一个文件夹
			fs.mkdirs(new Path(hdfsFilePath));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				fs.close();
			}
			catch (IOException e1)
			{
				fs = null;
				e1.printStackTrace();
			}
		}
	}
	
	
	
}
