package com.atguigu;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class Hbase_DML {
    //获取到Hbase连接
    public static Connection connection = HbaseConnectionSingle.connection;

    //插入数据
    public static void putCell(String namespace, String tableName, String rowKey, String family, String column, String value) throws IOException {
        //1.通过Connection获取Table对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //指定要往哪个列族，哪个列，插入值
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        //3.调用put方法写入数据
        table.put(put);

        //4.关闭Table对象
        table.close();

    }

    //获取数据
    public static void getCells(String namespace, String tableName, String rowKey, String family, String column) throws IOException {
        //1.通过Connection获取Table对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //获取所有可用的版本
        get.readAllVersions();

        //获取指定版本个数的数据
//        get.readVersions(2);

        //指定读取哪个列族中所有列的数据
//        get.addFamily(Bytes.toBytes(family));

        //指定读取哪个列族中哪一列的数据
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        //调用get方法获取数据
        Result result = table.get(get);

//        byte[] value = result.value();
//        System.out.println(Bytes.toString(value));

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //关闭连接
        table.close();
    }

    //删除数据
    public static void deleteCells(String namespace, String tableName, String rowKey, String family, String column) throws IOException {
        //1.通过Connection获取Table对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //删除指定列的所有版本
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));

        //删除指定列的最新版本
//        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));


        //调用delete方法删除数据
        table.delete(delete);

        //关闭连接
        table.close();
    }

    //扫描数据
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        //1.通过连接获取到Table对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //创建Scan对象
        Scan scan = new Scan();

        //获取所有版本的数据
        scan.setRaw(true);
        scan.readAllVersions();
        //读取指定版本个数的数据
//        scan.readVersions(10);

        //指定扫描范围 第二个boolean类型的参数代表的是包含不包含此行
//        scan.setStartRow(Bytes.toBytes(startRow));
        scan.withStartRow(Bytes.toBytes(startRow));

//        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.withStopRow(Bytes.toBytes(stopRow),true);

        //调用Scan获取扫描结果
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }


        //关闭Table
        table.close();
    }


    public static void main(String[] args) throws IOException {
//        putCell("bigdata", "student", "1001", "info", "age", "19");
//        putCell("bigdata", "student", "1001", "info", "age", "20");
//        putCell("bigdata", "student", "1001", "info", "age", "21");
//        putCell("bigdata", "student", "1001", "info", "age", "22");
//        putCell("bigdata", "student", "1001", "info", "age", "23");
//        putCell("bigdata", "student", "1001", "info", "age", "24");

//        deleteCells("bigdata", "student", "1001", "info", "age");

//        getCells("bigdata", "student", "1001", "info", "age");

        scanRows("bigdata", "student", "1001", "1004");

        //关闭Hbase连接
        HbaseConnectionSingle.closeConnection();
    }
}
