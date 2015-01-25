/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package twitter_sample;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.*;

import org.junit.Test;

import com.mysql.jdbc.Connection;

import twitter_sample.ConnectionPool;
import static twitter_sample.Twitter_sample.*;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

/**
 *
 * @author zhangyubao
 */
public class coveragetest {
    private static ConnectionPool conpool=null;
    
    @Test
    public void test_x() throws IOException, SQLException
    {
        //Session session = GetDBSSH();
        
       //int lport = 2026; //2022: search_ht, 2023:sample_dynamics, 2024: three_dyn, 2025: mytwitter1
       //GetDBSSH(lport);
//        
        
        int lport = 3306;
        String dbuserName = "root";
        String dbpassword = "";
        String url = "jdbc:mysql://127.0.0.1:"+lport+"/twitter1";
        String driverName="com.mysql.jdbc.Driver";
        Connection conn = (Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");
        
        
        Calculation.caltrend();
        Calculation.calsampledynamics();
        Calculation.extractcascade();
        //Search.tmpsearchdyn();
        Check.testUrls();
        Netinfo.NormalUrlHashtag();
        //Search.tmpsearchdyn();
    }
}
