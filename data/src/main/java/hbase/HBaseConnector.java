package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnector {
    // 目前共用一个连接，后续考虑使用连接池
    private static Connection connection = null;

    public static Connection getConnection() {
        if (connection == null) {
            synchronized (HBaseOperator.class) {
                if (connection == null) {
                    try {
                        Configuration config = HBaseConfiguration.create();
                        config.set("hbase.zookeeper.quorom", "127.0.0.1:2181");
                        connection = ConnectionFactory.createConnection(config);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        return connection;
    }
}
