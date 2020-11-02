import hdfs.FileRead;
import hdfs.RatingsProcess;
import hive.HiveConnector;

public class Main{
    public static void main(String[] args) throws Exception{
//        System.out.println("hello world");
//        FileRead fr = new FileRead();
//        fr.fileOperate();
        RatingsProcess rp = new RatingsProcess();
        rp.mapUserRating();
    }
}