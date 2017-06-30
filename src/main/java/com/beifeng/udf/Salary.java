package com.beifeng.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Decripsion:
 * @author by Coder_zhang
 * @date 	2017年6月21日		下午5:31:57		@version 1.0
 */
public class Salary extends UDF {
	public Text evaluate(Text salaryText){
		Text text = new Text();
		//1.判断salaryText是否为null
		if (salaryText == null) {
			return null;
		}
		//2.判断salaryText是否可转换为一个double类型
		double salary = 0;
		try {
			salary = Double.valueOf(salaryText.toString());
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return null;
		}
		if (salary > 3000) {
			text.set("大于3000的一组...");
			return text;
		}else if (salary <= 3000 && salary > 2000) {
			text.set("小于等于3000并且大于2000的一组...");
			return text;
		}else {
			text.set("小于等于2000的一组");
			return text;
		}
	}
}
