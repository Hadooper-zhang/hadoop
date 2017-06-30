package com.beifeng.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Decripsion:
 * @author by Coder_zhang
 * @date 	2017年6月21日		下午6:10:49		@version 1.0
 */
public class Clean extends UDF {
	public Text evaluate(Text field){
		if(field==null){
			return null;
		}
		if(StringUtils.isBlank(field.toString())){
			return null;
		}
		String str = field.toString();
		str = str.substring(1, str.length()-1);
		return new Text(str);
	}
}
