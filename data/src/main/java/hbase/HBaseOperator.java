package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.HashMap;
import java.util.Map;

public class HBaseOperator {


    public static Status insert(String rowKey, String family, String col, String value, String tableName) {
        if (tableName.length() == 0) {
            return new Status(false, "table name not specified");
        }
        Status status = new Status(true, "");
        Connection connection = HBaseConnector.getConnection();
        try {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), col.getBytes(), value.getBytes());
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (Exception e) {
            status.setOk(false);
            e.printStackTrace();
        }
        return status;
    }

    public static Map<String, String> get(String rowKey, String tableName) {
        Map<String, String> resultMap = new HashMap<>();
        Connection connection = HBaseConnector.getConnection();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            Result r = table.get(get);
            for (Cell cell : r.rawCells()) {
                resultMap.put(
                        String.format("%s:%s", new String(CellUtil.cloneFamily(cell)),
                                new String(CellUtil.cloneQualifier(cell))),
                        new String(CellUtil.cloneValue(cell)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMap;
    }
}
