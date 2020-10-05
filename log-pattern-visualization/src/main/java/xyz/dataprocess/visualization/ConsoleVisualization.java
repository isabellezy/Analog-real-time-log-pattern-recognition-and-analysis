package xyz.dataprocess.visualization;

import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.io.FileInputStream;

public class ConsoleVisualization {
    public static void main(String[] args) throws Exception{
        
        Properties prop=new Properties();
        FileInputStream ip= new FileInputStream("config.properties");
        prop.load(ip);
        
        String url = prop.getProperty("url");
        String user = prop.getProperty("user");
        String password = prop.getProperty("password");
        
        Connection con = DriverManager.getConnection(url, user, password);
        PreparedStatement pst = con.prepareStatement("SELECT pattern, sum(occurrence), max(id) from pattern where id > ? and time > now() - interval '5min' group by pattern");
            
        try{
            long id = -1;
            while(true) {
                Thread.sleep(100);
                long currentID = loadPatternsFromDB(pst, id);
                if (currentID != -1) {
                  id = currentID;
                }
            }
        }
        catch (java.lang.InterruptedException e) {
                return;
        }
    }
    public static long loadPatternsFromDB(PreparedStatement pst, Long id) {
   

        try {
            pst.setLong(1, id);
            ResultSet rs = pst.executeQuery();
            long maxID = -1;
            while(rs.next()) {
                System.out.println("Pattern:" + rs.getString(1) + ", Occurrences: " + rs.getInt(2));
                maxID = Math.max(maxID, rs.getLong(3));
            } 
            return maxID;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return -1;
        }
   }
}