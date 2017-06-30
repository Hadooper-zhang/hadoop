package com.beifeng.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

public class HBaseOperate {

	// 1.创建表
	@Test
	public void createTable() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		String tbName = "test";
		String colFamaily = "info";

		// 获取配置信息 hbase-site.xml
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost.localsingle");

		// 管理员
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);

		// 表名称对象
		TableName tableName = TableName.valueOf(tbName);

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(new HColumnDescriptor(colFamaily));

		// 创建一个表
		hbaseAdmin.createTable(tableDesc);
		System.out.println("创建表" + tableName + "成功");
	}
}
