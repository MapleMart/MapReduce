package com.hyf.mapreduce.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 由于nginx的日期时间格式比较特殊，如果没有对nginx做格式配置，默认的日期时间无法用常规的类进行解析，因此需要做一个转化工具。
 * 为了使用方便，我们将nginx的日期时间解析成2类数据，一个只包含年月日，另一个不仅包含年月日，而且包含时分秒，介于包含了时分秒后字
 * 符串格式相对不固定（具体得看需求），所以包含时分秒的时间我们不做format处理，仅仅提供long的时间戳数据，使用的时候随时可以转化。
 * @author 黄永丰
 * @createtime 2015年12月22日
 * @version 1.0
 */
public class AnalysisNginxTool
{
	private static Logger logger = LoggerFactory.getLogger(AnalysisNginxTool.class);

	public static String nginxDateStmpToDate(String date)
	{
		String res = "";
		try
		{
			SimpleDateFormat df = new SimpleDateFormat("[dd/MM/yyyy:HH:mm:ss");
			String datetmp = date.split(" ")[0].toUpperCase();
			String mtmp = datetmp.split("/")[1];
			datetmp = datetmp.replaceAll(mtmp, (String) DateToNUM.map.get(mtmp));

			Date d = df.parse(datetmp);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			res = sdf.format(d);
		}
		catch (ParseException e)
		{
			logger.error("error:" + date, e);
		}
		return res;
	}

	public static long nginxDateStmpToDateTime(String date)
	{
		long l = 0;
		try
		{
			SimpleDateFormat df = new SimpleDateFormat("[dd/MM/yyyy:HH:mm:ss");
			String datetmp = date.split(" ")[0].toUpperCase();
			String mtmp = datetmp.split("/")[1];
			datetmp = datetmp.replaceAll(mtmp, (String) DateToNUM.map.get(mtmp));

			Date d = df.parse(datetmp);
			l = d.getTime();
		}
		catch (ParseException e)
		{
			logger.error("error:" + date, e);
		}
		return l;
	}
	
	
}
