package hive;

import dto.Info;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public class HiveOperate {
    private  static Logger logger = Logger.getLogger(HiveOperate.class);
    public  static void insertUserMovies(List<Info> infoList, long userId, String tableName){
        try{
            Connection connection = HiveConnector.getConnection();
            // insert into table values(
            String sql = String.format("insert into %s values(%d,'%s')",tableName,userId,infoList.toString());
            Statement statement = connection.createStatement();
            boolean ok = statement.execute(sql);
            if (!ok){
                logger.warn("fail to write hive");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
