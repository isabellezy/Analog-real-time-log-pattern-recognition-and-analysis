package xyz.dataprocess.matcher;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Properties;

public class PatternMatcher extends RichFlatMapFunction<LogEntry, Tuple3<Double, String, Integer>> {
    //String[] patternlist = {"BLOCK* NameSystem.addStoredBlock", "Received block"};
    private ArrayList<String> patternlist;
    static Logger LOG = LoggerFactory.getLogger(PatternMatcher.class);
    private String url;
    private String user;
    private String password;
    private int topKnum;

    private class MyReloadThread extends Thread {
         public void run() {
            try {
                while (true) {
                    Thread.sleep(15000);
                    loadPatternsFromDB();
                }
            } catch (java.lang.InterruptedException e) {
                return;
            }
        }
    }
    
    public PatternMatcher(Properties prop) {
        this.url = prop.getProperty("dburl");
        this.user = prop.getProperty("dbusername");
        this.password = prop.getProperty("dbpassword");
        this.topKnum = Integer.parseInt(prop.getProperty("topKnum"));
    }
    
    @Override
    public void open(org.apache.flink.configuration.Configuration conf) throws Exception {
        loadPatternsFromDB();
        new MyReloadThread().start();
        super.open(conf);
    }
    
    @Override
    public void flatMap(LogEntry logEntry, Collector<Tuple3<Double, String, Integer>> out) {
        ArrayList<String> patternlistCopy = null;
        
        
        synchronized (this) {
            patternlistCopy = patternlist;
        }
        

        for (String  pattern : patternlistCopy) {
            if (logEntry.log.contains(pattern)) {

                out.collect(new Tuple3<>(logEntry.timestamp, pattern, 1));
            }
        }
   }
   
   private void loadPatternsFromDB() {
        try {
            Connection con = DriverManager.getConnection(url, user, password);
            PreparedStatement pst = con.prepareStatement("SELECT DISTINCT PATTERN FROM (SELECT PATTERN FROM RANKEDPATTERN ORDER BY TIME DESC LIMIT ?) AS T");
            pst.setInt(1, 2 * topKnum);
            ResultSet rs = pst.executeQuery();
            ArrayList<String> newPatternList = new ArrayList<>();
            
            while (rs.next()) {
                newPatternList.add(rs.getString(1));
            }
            pst.close();
            con.close();

            synchronized (this) {
                patternlist = newPatternList;
            
                
            }
            
            LOG.info("Patterns loaded from database.");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
   }
   
 /*  private void loadPatterns() {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        try {
                S3Object o = s3.getObject("yuanzhou-log-dataset", "log-pattern.csv");
                S3ObjectInputStream s3is = o.getObjectContent();
                CSVReader csvReader = new CSVReader(new InputStreamReader(s3is));
                
                patternlist = new ArrayList();
                String[] row;
                while ((row = csvReader.readNext()) != null) {
                    patternlist.add(row[0]);
                }
                s3is.close();
                csvReader.close();
                
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (CsvValidationException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
   } */
}