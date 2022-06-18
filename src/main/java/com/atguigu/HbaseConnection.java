package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnection {
    public static void main(String[] args) throws IOException {
        //0.设置配置项
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //1.获取Hbase连接
        Connection connection = ConnectionFactory.createConnection(conf);

        //2.打印连接
        System.out.println(connection);

        //3.关闭连接
        connection.close();
    }
}
