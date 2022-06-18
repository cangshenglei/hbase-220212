package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnectionSingle {
    //设置Hbase连接
    public static Connection connection = null;

    static {
        if (connection==null){
            Configuration configuration = new Configuration();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭连接
    public static void closeConnection() throws IOException {
        if (connection!=null){
            connection.close();
        }
    }


    public static void main(String[] args) throws IOException {


        System.out.println(connection);

        HbaseConnectionSingle.closeConnection();
    }
}
