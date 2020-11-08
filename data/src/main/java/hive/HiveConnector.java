package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveConnector {
    private static  final String driver = "org.apache.hive.jdbc.HiveDriver";
    private static  final String url = "jdbc:hive2://localhost:10000/data";
    private static  Connection con = null;
    public static Connection getConnection(){
        if(con == null){
            synchronized (HiveConnector.class){
                if(con == null){
                    try{
                        Class.forName(driver);
                        con = DriverManager.getConnection(url,"root","123456");
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
        return con;
    }

}
