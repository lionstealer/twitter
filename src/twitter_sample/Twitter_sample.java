/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitter_sample;

//import weiboprocess.ConnectionPool;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.log4j.PropertyConfigurator;
//import com.jmatio.io.MatFileReader;
//import com.jmatio.types.MLArray;
//import com.jmatio.types.MLDouble;
/**
 *
 * @author zhangyubao
 */
public class Twitter_sample {

    /**
     * @param args the command line arguments
     */
    
    private static ConnectionPool conpool=null;
    private static LevenshteinDistance ldistance = null;
    private static ByteBuffer bb;  //
    private static ByteBuffer bb_search;  //
    private static ScheduledExecutorService SampleThreadPool = null;
    private static ScheduledExecutorService SearchThreadPool = null;
    
    
    public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
        // TODO code application logic here
        
        //Session session = GetDBSSH();
        
       //int lport = 2026; //2022: search_ht, 2023:sample_dynamics, 2024: three_dyn, 2025: mytwitter1
       //GetDBSSH(lport);
//        
        
        int lport = 3306;
        String dbuserName = "root";
        String dbpassword = "";
        String url = "jdbc:mysql://localhost:"+lport+"/twitter1";
        String driverName="com.mysql.jdbc.Driver";
        
       
        try{
            conpool = new ConnectionPool(driverName,url,dbuserName,dbpassword);
            conpool.createPool();
        
        }catch(Exception e)
        {
            System.out.println("Failed to create conn pool!!!!!");
        }
        
        ///////////////////////////////
        
        SampleThreadPool=Executors.newScheduledThreadPool(1);
        
        //search2DB search = new search2DB();
//        int Sampletread=1;
//        for(int i = 0;i<Sampletread;i++)
//        {
//            SampleThreadPool.schedule(Sample2DBRun, i+1, TimeUnit.MINUTES);
//        }
//        
//        SearchThreadPool=Executors.newScheduledThreadPool(2);
//        
//        //search2DB search = new search2DB();
//        int Searchtread=2;
//        for(int i = 0;i<Searchtread;i++)
//        {
//            SearchThreadPool.schedule(Search2DBRun, i+1, TimeUnit.MINUTES);
//        }


        
        /////////////////////////////////////dynamics//////////////////////////////////////
        //getNetRel();
        //inferht();  //calculate the search dynamics info from twitter1 (for sample topics)
        //caltrend();  //calculate the 
        //calsampledynamics();   //calculate the sample dynamics info from twitter1 (for topics > 5)
        //movesharpfromtrend();
        //tmpsearchdyn(); //calculate the sample dynamics info from twitter_tmp (for a certain topic)
        
        
        /////////////////////////////////////past functions//////////////////////////////////////
            //session.disconnect();
        //trend_time_array();
        //NormalUrlHashtag();
        //gettbtrendpos();
        //getQuerySearch();
        //queryUrlMention();
        //sample_time_array();
        //getMentionNetwork();
        //getMatrixfromDB();
        
        //testUrls();
        //getuserspaminfo();
        //getpotentialscale();
        //calreciprocity();
        //writeresponsetimesamples();
        //extractcascade();
        //checkfollowing();
        //traceconscutivetrend();
        //measuretrendtime();
        //getSampleThread();
        //uniteSearchdynamics();
        //outputdynamics();
        //DBoperate();
        //outputdata();

        //outputTweetforTMT();
        //processtxtfile();
        coveragetest ct=new coveragetest();
        ct.test_x();
        return;
    }
   
    public static void inferht() throws SQLException
    {
        Connection conn=conpool.getConnection();
        
        //get pos
        Statement stat = conn.createStatement();
        String q = "select processpos from tb_position where name='searchdyntime';";
        ResultSet rs = stat.executeQuery(q);
        rs.next();
        int pos=rs.getInt(1);
        rs.close();
        stat.close();
        
        while(true)
        {
            //get trending topics and time
            //int pos=0;
            String[] topics=new String[15];
            int[] dynamics=new int[15];
            Timestamp ts=null;
            Statement stat1 = conn.createStatement();
            String q1 = "select t_name,t_inserttime,id from tb_trend where id>"+pos+" and t_type like 'Sample%' limit 15;";
            ResultSet rs1 = stat1.executeQuery(q1);
            int idx=0;
            while(rs1.next())
            {
                topics[idx]=rs1.getString(1); //get topics
                ts=rs1.getTimestamp(2); //limit time
                pos=rs1.getInt(3); //renew pos
                idx++;
            }
            rs1.close();
            stat1.close();
            
            //update the pos of trend
            Statement stat0=conn.createStatement();
            String q0="update tb_position set processpos='"+pos+"' where name='searchdyntime';";
            stat0.execute(q0);
            stat0.close();
           
            
            //infer the ht
            Timestamp t_start=ts;
            long time = ts.getTime();
            time=time + 30*60*1000;
            Timestamp t_end=new Timestamp(time);
            t_end.setTime(time);
            
//            Statement stat2=conn.createStatement();
//            String q2="select mbloginfoId from tb_search where publishTime>='"+t_start+"' limit 1;";
//            ResultSet rs2=stat2.executeQuery(q2);
//            rs2.next();
//            int startid=rs2.getInt(1);
//            rs2.close();
//            stat2.close();
//            
//            Statement stat3=conn.createStatement();
//            String q3="select mbloginfoId from tb_search where publishTime>='"+t_end+"' limit 1;";
//            ResultSet rs3=stat3.executeQuery(q3);
//            rs3.next();
//            int endid=rs3.getInt(1);
//            rs3.close();
//            stat3.close();
            
            for(int i=0;i<15;i++)
            {
                //each topic
                
                //get frequency
                Statement stat4=conn.createStatement();
                String q4="select count(*) from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"';";
                ResultSet rs4=stat4.executeQuery(q4);
                rs4.next();
                int cnt=rs4.getInt(1);
                rs4.close();
                stat4.close();
                
                Timestamp t1=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t1+"]: get frequency...");
                
                
                //get user number
                Statement stat5=conn.createStatement();
                String q5="select count(distinct userId) from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"';";
                ResultSet rs5=stat5.executeQuery(q5);
                rs5.next();
                int usercnt=rs5.getInt(1);
                rs5.close();
                stat5.close();
                
                Timestamp t2=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t2+"]: get user number...");


                //get rt number
                Statement stat6=conn.createStatement();
                String q6="select count(*) from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"' and rt=1;";
                ResultSet rs6=stat6.executeQuery(q6);
                rs6.next();
                int rtcnt=rs6.getInt(1);
                rs6.close();
                stat6.close();
                
                Timestamp t3=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t3+"]: get rt number...");

                //get mt number
                Statement stat7=conn.createStatement();
                String q7="select count(*) from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"' and mt<>0;";
                ResultSet rs7=stat7.executeQuery(q7);
                rs7.next();
                int mtcnt=rs7.getInt(1);
                rs7.close();
                stat7.close();
                
                Timestamp t4=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t4+"]: get mt number...");

                //get follow count
                int followcnt=0;
                Statement stat8=conn.createStatement();
                String q8="select followcount from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"';";
                ResultSet rs8=stat8.executeQuery(q8);
                while(rs8.next())
                {
                    int fcnt=rs8.getInt(1);
                    followcnt=followcnt+fcnt;
                }
                
                rs8.close();
                stat8.close();
                
                Timestamp t5=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t5+"]: get follow count...");

                //get tweet count
                int tweetcnt=0;
                Statement stat9=conn.createStatement();
                String q9="select tweetcount from tb_search_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='"+topics[i]+"';";
                ResultSet rs9=stat9.executeQuery(q9);
                while(rs9.next())
                {
                    int tcnt=rs9.getInt(1);
                    tweetcnt=tweetcnt+tcnt;
                }
                
                rs8.close();
                stat8.close();
                
                Timestamp t6=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t6+"]: get tweet count...");
                
                
                //insert topic
                Statement stat_in=conn.createStatement();
                String q_in="insert into tb_search_topicdyn(hashtag,frequency,timeinfo,usernumber,rtnumber,mtnumber,"
                        + "followcount,tweetcount)"
                        + " values ('"+topics[i]+"','"+cnt+"','"+t_start+"','"+usercnt+"','"+rtcnt+"','"+mtcnt+"',"
                        + "'"+followcnt+"','"+tweetcnt+"')";
                stat_in.execute(q_in);
                stat_in.close();
                
                //System.out.println(topics[i]+" at time "+t_start+" done..... from "+startid+"("+t_start+") to "+endid+" ("+t_end+")");
            }
            
            
        }
    }
    
    
//    public static void getUserTime() throws FileNotFoundException, IOException, SQLException
//    {
//        Connection conn = conpool.getConnection();
//        //new int[9];
//        String dir = "/Users/zhangyubao/Documents/twitterresult/";
//        MatFileReader matfilereader = new MatFileReader(dir+"fbexecutors.mat");
//        //MLArray fbexecutor = matfilereader.getMLArray("fbexecutor");
//        MLDouble mlfbexecutor = (MLDouble)matfilereader.getMLArray("fbexecutor");
//        //double[][] fbexecutor = mlfbexecutor.getArray();
//        //fbexecutor.
//        for(int i=0;i<mlfbexecutor.getSize();i++)
//        {
//            double drank=mlfbexecutor.get(i);
//            int rank=(int)drank;
//            
//            Statement stat1=conn.createStatement();
//            String q1="select name from fb_hostlist where id='"+rank+"';";
//            ResultSet rs1=stat1.executeQuery(q1);
//            rs1.next();
//            String userid=rs1.getString(1);
//            rs1.close();
//            stat1.close();
//            
//            
//            String q2="select count(distinct microblogId,publishTime) from tb_search where userId='"+userid+"';";
//            Statement stat2=conn.createStatement();
//            ResultSet rs2 = stat2.executeQuery(q2);
//            rs2.next();
//            int ptcnt=rs2.getInt(1);
//            rs2.close();
//            stat2.close();
//            
//            long[] ptimearray=new long[ptcnt];
//            String q3="select distinct microblogId,publishTime from tb_search where userId='"+userid+"';";
//            Statement stat3=conn.createStatement();
//            ResultSet rs3 = stat3.executeQuery(q3);
//            int k=0;
//            while(rs3.next())
//            {
//                
//                String tmp = rs3.getString(2);
//                Timestamp t = Timestamp.valueOf(tmp);
//                ptimearray[k]=t.getTime()/1000;
//                k++;
//            }
//            
//            
//            
//        }
//        
//    }
      
    
    public static void trend_time() throws SQLException
    {
        Connection conn1 = conpool.getConnection();
        
        Statement stat1 = conn1.createStatement();
        String qry1 = "select t_name,t_type from tb_trend_pos";
        ResultSet rs1 = stat1.executeQuery(qry1);
        while(rs1.next())
        {
            String name = "";
            String type = "";
            name = rs1.getString(1);
            type = rs1.getString(2);
            
            Statement stat2 = conn1.createStatement();
            String qry2 = "select min(t_inserttime),max(t_inserttime) from tb_trend where t_name='"+name+"' and t_type='"+type+"';";// and t_inserttime > '2013-02-05 00:00:00'
            ResultSet rs2 = stat2.executeQuery(qry2);
            
            rs2.next();
            String t1;
            String t2;
            t1=rs2.getString(1);
            t2=rs2.getString(2);
            
            rs2.close(); stat2.close();
            
            Statement stat3 = conn1.createStatement();
            String qry3 = "update tb_trend_pos set t_init_time = '"+t1+"',t_end_time='"+t2+"' where t_name='"+name+"' and t_type='"+type+"';";
            stat3.execute(qry3);
            stat3.close();
            
        }
        rs1.close();
        stat1.close();
    }
    
    
    public static void  processTrend() throws SQLException
    {
        Connection connection = conpool.getConnection();
        Statement trend_state = connection.createStatement();
        String sql1 = "select * from tb_trend;";
        ResultSet rs = trend_state.executeQuery(sql1);
        //to lower case and delete #
        while(rs.next())
        {
            int id = rs.getInt(1);
            String tname = rs.getString(2);
            tname = tname.toLowerCase();
            
            if(tname.startsWith("#"))
            {
                tname = tname.substring(1,tname.length());
            }
            Statement statesharp = connection.createStatement();
            String sql2 = "update tb_trend set t_name ='"+tname+ "' where id = '"+id+"';";
            statesharp.execute(sql2);
            statesharp.close();

        }
        
//        trend_state.close();
//        
//        Statement simple_state = connection.createStatement();
//        String sql3 = "select t_name,t_type,t_inserttime from tb_trend";
//        ResultSet rs1 = trend_state.executeQuery(sql3);
//        
//        while(rs1.next())
//        {
//            
//        }
        
    }

    
    public static Session GetDBSSH(int lport)
    {
        
        //ssh remote mysql database
        //int lport=2022;
        //int lport = 2022;
        String rhost="127.0.0.1";
        //String host="128.239.132.254";
        String host = "rocco.cs.wm.edu";
        int rport=3306;
        String user="wangserv";
        String password="kvmwang";
        String dbuserName = "root";
        String dbpassword = "";
        String url = "jdbc:mysql://localhost:"+lport+"/twitter";
        String driverName="com.mysql.jdbc.Driver";
        java.sql.Connection connection = null;
        Session session= null;
        
        ///////////////////////////////
        try{
            java.util.Properties config = new java.util.Properties(); 
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            //session=jsch.getSession(user, host, 65022);
            session=jsch.getSession(user, host, 6166);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
            System.out.println("Connected");
            int assinged_port=session.setPortForwardingL(lport, rhost, rport);
            System.out.println("localhost:"+assinged_port+" -> "+rhost+":"+rport);
            System.out.println("Port Forwarded");
            
            //mysql database connectivity
       
            Class.forName(driverName).newInstance();
            connection = DriverManager.getConnection (url, dbuserName, dbpassword);
            
            
            
        }catch(Exception e)
        {
            e.printStackTrace();
        }
        //Session_Connection sc = new Session_Connection(session,connection);
        
        return session;
    }


     
     
}


