import java.util.List;

import hdfs.TagProcess;
import utils.NlpUtil;


public class Main{

    public static void main(String[] args) throws Exception{
//        System.out.println("hello world");
//        FileRead fr = new FileRead();
//        fr.fileOperate();
//        RatingsProcess rp = new RatingsProcess();
//        rp.mapUserRating();

        // instantiate Configuration class
//
//        HBaseDemo hBaseDemo = new HBaseDemo();
//        hBaseDemo.testConnection();

        // close HTable instance
        List<String> l = NlpUtil.getLema("these kids are playing");
        System.out.println(l.toString());
        TagProcess tp = new TagProcess();
        tp.process();
    }
}