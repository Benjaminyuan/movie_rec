import java.io.IOException;

import hbase.HBaseDemo;
import hdfs.RatingsProcess;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Main{
    public static void main(String[] args) throws Exception{
//        System.out.println("hello world");
//        FileRead fr = new FileRead();
//        fr.fileOperate();
        RatingsProcess rp = new RatingsProcess();
        rp.mapUserRating();

        // instantiate Configuration class
//
//        HBaseDemo hBaseDemo = new HBaseDemo();
//        hBaseDemo.testConnection();

        // close HTable instance
    }
}