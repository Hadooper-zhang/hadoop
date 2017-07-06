package com.beifeng.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Decripsion:
 * @author by Coder_zhang
 * @date 	2017年7月6日		上午10:05:36		@version 1.0
 */
public class DateTransformUDF extends UDF{

	private final SimpleDateFormat inputFormat =  new
			SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);	
	private final SimpleDateFormat outputFormat = new
			SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Text outputDate= new Text();
	
	//"31/Aug/2015:00:04:37 +0800" -> String -> Date -> String -> Text 
	public  Text evaluate(Text inputColumn){
		//过滤
		if(inputColumn == null){
			return null;
		} 
		
		//"31/Aug/2015:00:04:37 +0800" -> String
		String column = inputColumn.toString();
		String str = column.substring(1, column.length()-1);
		
		Date inputDate = null;
		try {
			inputDate = inputFormat.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String output = outputFormat.format(inputDate);
		
		outputDate.set(output);
		
		return outputDate;		
	}
	
	public static void main(String[] args) {
		System.err.println(new DateTransformUDF().evaluate(new Text("\"31/Aug/2015:00:04:37 +0800\"")));
	}
}
