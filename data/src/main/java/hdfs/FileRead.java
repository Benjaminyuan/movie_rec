package hdfs;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class FileRead {
    public void fileOperate() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        boolean ok = fs.mkdirs(new Path("hdfs:/mydir"));
        if (ok) {
            System.out.println("success to create dir");
        } else {
            System.out.println("success to create dir");
        }
        FSDataOutputStream outStream = fs.create(new Path("hdfs:/mydir/wordcount.txt"));
        outStream.write("ttt  tta  a afsdf dasf ".getBytes());
        outStream.close();
        InputStream inStream = fs.open(new Path("hdfs:/mydir/wordcount.txt"));
        IOUtils.copyBytes(inStream,System.out,1024,false);
        inStream.close();
        fs.close();
    }

}