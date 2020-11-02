package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.sql.Connection;

public class HBaseConnector {
    public void getConnector(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://127.0.0.1:9000/hbase");
        conf.set("hbase.zookeeper.property.clientPort","2180");
        conf.addResource("");
//        ConnectionFactory.createConnection(conf);
//        conf.set("");
    }

}
