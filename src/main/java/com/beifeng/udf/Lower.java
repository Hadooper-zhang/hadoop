package com.beifeng.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Decripsion:
 * @author by Coder_zhang
 * @date 	2017年6月16日		下午3:24:06		@version 1.0
 */
public class Lower extends UDF{
	public Text evaluate(Text str){
		if(null==str){
			return null;
		}
		if(StringUtils.isBlank(str.toString())){
			return null;
		}
		return new Text(str.toString().toLowerCase());
	}
//	public static void main(String[] args) {
//		System.out.println(new Lower().evaluate(new Text("SAHDKJSAHDJKA")));
//	}
	
}
