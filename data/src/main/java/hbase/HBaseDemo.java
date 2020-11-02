package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {
    public void testConnection(){
        try{
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorom","127.0.0.1:2181");
            Connection connection = ConnectionFactory.createConnection(config);
            // instantiate HTable class
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("test");
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes());
            builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            TableDescriptor tableDescriptor = builder.build();
            admin.createTable(tableDescriptor);
            // instantiate Put class
            Table table = connection.getTable(tableName);
            Put p = new Put(Bytes.toBytes("row001"));
            p.addColumn("f1".getBytes(),"info".getBytes(),"tttt".getBytes());
            table.put(p);
            // add values using add() method
            Get get = new Get("row001".getBytes());
            Result r = table.get(get);
            for(Cell cell : r.rawCells()){
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(String.format("%s,%s,%s",family,qualifier,value));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
