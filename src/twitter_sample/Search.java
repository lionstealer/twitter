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
public class Search {

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
        String url = "jdbc:mysql://127.0.0.1:"+lport+"/twitter1";
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
    
    public static void tmpsearchdyn() throws SQLException
    {
        Connection conn=conpool.getConnection();
        
        int currentpos=0;
        
        //get pos
        Statement stat = conn.createStatement();
        String q = "select processpos from tb_position where name='searchdyntime';";
        ResultSet rs = stat.executeQuery(q);
        rs.next();
        int pos=rs.getInt(1);
        rs.close();
        stat.close();
        
        currentpos=pos;
        //get time
        Statement stat1 = conn.createStatement();
        String q1 = "select id,timeinfo from tb_globaldyn where id>='"+pos+"';";
        ResultSet rs1 = stat1.executeQuery(q1);
        while(rs1.next())
        {
            int tpos=rs1.getInt(1);
            currentpos=tpos;
            Timestamp t_start=rs1.getTimestamp(2);
            
            long time = t_start.getTime();
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
            
                
                //get frequency
                Statement stat4=conn.createStatement();
                String q4="select count(*) from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey';";
                ResultSet rs4=stat4.executeQuery(q4);
                rs4.next();
                int cnt=rs4.getInt(1);
                rs4.close();
                stat4.close();
                
                Timestamp t1=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t1+"]: get frequency...");
                
                
                //get user number
                Statement stat5=conn.createStatement();
                String q5="select count(distinct userId) from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey';";
                ResultSet rs5=stat5.executeQuery(q5);
                rs5.next();
                int usercnt=rs5.getInt(1);
                rs5.close();
                stat5.close();
                
                Timestamp t2=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t2+"]: get user number...");


                //get rt number
                Statement stat6=conn.createStatement();
                String q6="select count(*) from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey' and rt=1;";
                ResultSet rs6=stat6.executeQuery(q6);
                rs6.next();
                int rtcnt=rs6.getInt(1);
                rs6.close();
                stat6.close();
                
                Timestamp t3=new Timestamp(System.currentTimeMillis());
                System.out.println("["+t3+"]: get rt number...");

                //get mt number
                Statement stat7=conn.createStatement();
                String q7="select count(*) from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey' and mt<>0;";
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
                String q8="select followcount from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey';";
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
                String q9="select tweetcount from tb_sample_dyninfo where "
                        + "httime<'"+t_end+"' and httime>='"+t_start+"' and "
                        + "hashtag='OperationVoteCollinsKey';";
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
                String q_in="insert into tb_sample_topicdyn(hashtag,frequency,timeinfo,usernumber,rtnumber,mtnumber,"
                        + "followcount,tweetcount)"
                        + " values ('OperationVoteCollinsKey','"+cnt+"','"+t_start+"','"+usercnt+"','"+rtcnt+"','"+mtcnt+"',"
                        + "'"+followcnt+"','"+tweetcnt+"')";
                stat_in.execute(q_in);
                stat_in.close();
        }
        
        Statement stat10 = conn.createStatement();
        String q10 = "update tb_position set processpos='"+currentpos+"' where name='searchdyntime';";
        stat10.execute(q10);
        stat10.close();
       
        rs1.close();
        stat1.close();
    }
         
    public static void getpotentialscale() throws SQLException, FileNotFoundException, IOException
    {    
        Connection conn = conpool.getConnection();
        
        String q1= "select userId,publishTime from tb_sample_evoltopics where ";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/zhangyubao/Documents/twitterresult/evoltopicsquery.txt")));
        String data = null;
        while((data = br.readLine())!=null)
        {
          //System.out.println(data); 
            
            //////////////////GET hashtag name/////////////////////
            String[] htarray = data.split("%");
            String htname = htarray[1];
            System.out.println(htname);
            
            Statement stat2 = conn.createStatement();
            String q2=q1+data+" order by publishTime asc;";
            
            ResultSet rs2 = stat2.executeQuery(q2);
            while(rs2.next())
            {
                String uid = rs2.getString(1);
                String time = rs2.getString(2);
                
                System.out.println(uid+" "+time);
                
                Statement stat3 = conn.createStatement();
                String q3="select spaminfo from tb_sample_users where userId='"+uid+"';";
                ResultSet rs3=stat3.executeQuery(q3);
                rs3.next();
                int spam=rs3.getInt(1);
                System.out.println("spam:"+spam);
                if(0==spam)
                {
                    Statement stat4 = conn.createStatement();
                    String q4 = "select followcount,friendcount,tweetcount from tb_sample_users where userId='"+uid+"';";
                    ResultSet rs4 = stat4.executeQuery(q4);
                    rs4.next();
                    int followcount=rs4.getInt(1);
                    int friendcount=rs4.getInt(2);
                    int tweetcount=rs4.getInt(3);
                    
                    String tofile = followcount+" "+friendcount+" "+tweetcount+" "+time+"\n";
                    
                    System.out.println(tofile);
                    
                    
                    rs4.close();
                    stat4.close();
                }else
                {
                    
                }
                
                rs3.close();
                stat3.close();
            }
            rs2.close();
            stat2.close();
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
    
    public static void findUsers() throws SQLException
    {
        Connection conn = conpool.getConnection();
        
        Statement stat = conn.createStatement();
        String q = "select count(*) from tb_tmp";
        
        ResultSet rs = stat.executeQuery(q);
        rs.next();
        int count = rs.getInt(1);
        int pos = 33004;
        while(pos<count)
        {
            int limit = 1;
            if(pos+1>count)
            {
                limit = count-pos;
            }
            
            String q2 = "select userId from tb_tmp limit "+limit +" offset "+pos+";";
            Statement stat2 = conn.createStatement();
            ResultSet rs2 = stat2.executeQuery(q2);
            rs2.next();
            String uid = rs2.getString(1);
            rs2.close();
            stat2.close();
                    
        
            Statement stat1 = conn.createStatement();

            String q1 = "select id from tb_search_users where userId ='"+uid+"';";

            ResultSet rs1 = stat1.executeQuery(q1);

            while(rs1.next())
            {
                int id = rs1.getInt(1);
                try{
                    System.out.println(id);
                        Statement stat3 = conn.createStatement();
                        String q3 = "update tb_search_users set getFriend=0 where id ='"+id+"';";
                        stat3.execute(q3);

                         stat3.close();
                    }catch(Exception e)
                    {}
            }
            rs1.close();
            stat1.close();
            
            pos=pos+1;
        }
        
        conn.close();
    }
     
     
}


