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
public class Netinfo {

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
    
    public static void getNetRel() throws SQLException
    {
        Connection conn=conpool.getConnection();
        
        try{
        Statement stat1=conn.createStatement();
//        String q1="insert into tb_sample_htfornetrel (select * from tb_sample where `hashtag1`='mtvhottest' "
//                + "or `hashtag2`='mtvhottest' or `hashtag1`='20factsaboutme' or `hashtag2`='20factsaboutme' "
//                + "or `hashtag1`='wecantdateif' or `hashtag2`='wecantdateif' or `hashtag1`='MentionSomeoneHandsome' "
//                + "or `hashtag2`='MentionSomeoneHandsome' or `hashtag1`='mentionsomeonebeautiful' or "
//                + "`hashtag2`='mentionsomeonebeautiful' or `hashtag1`='SingleBecause' or "
//                + "`hashtag2`='SingleBecause' or `hashtag1`='TalkAboutYourCrush' or `hashtag2`='TalkAboutYourCrush' "
//                + "or `hashtag1`='easilyattractedto' or `hashtag2`='easilyattractedto')";
        String q1="select content,hashtag2,publishTime from tb_sample_htfornetrel where content like 'RT @%';";
        ResultSet rs1=stat1.executeQuery(q1);
        while(rs1.next())
        {
            
            String content=rs1.getString(1);
            String id=content.split(":",-2)[0];
            id=id.substring(4, id.length());
            
            if(true)
            {
                String ht=rs1.getString(2);
                if(!"".equals(ht))
                {
                    Timestamp t=rs1.getTimestamp(3);

                    Statement stat2=conn.createStatement();
                    String q2="select * from tb_sample_htfornetrel where userId ='"+id+"';";
                    ResultSet rs2=stat2.executeQuery(q2);
                    if(rs2.next())
                    {
                        String user=rs2.getString("userId");
                        long l=rs2.getLong("userId_long");
                        System.out.println(user);

                        Statement stat3=conn.createStatement();
                        String q3="insert into tb_netrel(hashtag,relnum,timeinfo) values('"+ht+"','"+l+"','"+t+"');";
                        stat3.execute(q3);
                        stat3.close();


                    }
                    stat2.close();
                }
            }
        }
        stat1.close();
        }catch(SQLException s)
        {
            System.out.println(s);
        }
        System.out.println("Over!!!!");
       
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
    
    public static void NormalUrlHashtag() throws SQLException
    {
        Connection conn = conpool.getConnection();
        Statement stat1 = conn.createStatement();
        String q1 = "select count(mbloginfoId) from tb_search_ht";
        
        ResultSet rs1 = stat1.executeQuery(q1);
        rs1.next();
        int count = rs1.getInt(1);
        int pos = 0;
        while(pos<count)
        {
            String q4="";
            if(pos+10000<count)
            {
                q4 = "select mbloginfoId, content, Url, hashtag from tb_search_ht limit 10000 offset "+pos;
            }else
            {
                q4 = "select mbloginfoId, content, Url, hashtag from tb_search_ht limit "+(count-pos)+" offset "+pos;
            }
            Statement stat4 = conn.createStatement();
            ResultSet rs = stat4.executeQuery(q4);

            while(rs.next())
            {
                int id = rs.getInt(1);
                String c = rs.getString(2);
                String u = rs.getString(3);
                String h = rs.getString(4);

                ////////////////////////////hashtag///////////////////////////////
//                if(h.startsWith("#"))
//                {
//                    String ht = h.substring(1, h.length());
//                    try{
//                        System.out.println(id +"   "+h+"  -  "+ht);
//                        Statement stat2 = conn.createStatement();
//                        String q2 = "update tb_search set hashtag ='"+ht+"' where mbloginfoId ='"+id+"';";
//                        stat2.execute(q2);
//                        stat2.close();
//                    }catch(Exception e)
//                    {}
//
//                }
                
                
                ////////////////////////////////retweet/////////////////////////////////
                System.out.println(id);
                if(c.startsWith("RT @") || c.startsWith("rt @")||c.startsWith("RETWEET @")||c.startsWith("retweet @"))
                {
                    String rtfrom = "";
                    String[] tmp = c.split(":",2);
                    rtfrom = tmp[0];
                    rtfrom = rtfrom.substring(4, rtfrom.length());
                    try{
                           
                            Statement stat3 = conn.createStatement();
                            String q3 = "update tb_search_ht set retweetId='"+rtfrom+"' where mbloginfoId='"+id+"';";
                            stat3.execute(q3);
                            stat3.close();
                        }catch(Exception e)
                        {}
                    
                }
                
                
                
                

                /////////////////////////////////Url//////////////////////////////
                if(c.contains("http://")||true)
                {
                    String url = "";
                    String tmpht = "";
                    String tmp = "";
                    
                    StringTokenizer tokens = new StringTokenizer(c, " ");
                    
                    while(tokens.hasMoreTokens())
                    {
                        tmp=tokens.nextToken();
                        if(tmp.startsWith("http://"))
                        {
                            url = tmp;
                            //break;
                        }else if(tmp.startsWith("#"))
                        {
                            if(tmpht=="")
                            {
                                tmpht = tmp.substring(1,tmp.length());
                            }else{
                                tmpht = tmpht+","+tmp.substring(1,tmp.length());
                            }
                            
                        }else
                        {
                            
                        }
                        
                    }
                    
                        try{
                            System.out.println(url);
                            Statement stat3 = conn.createStatement();
                            String q3 = "update tb_search_ht set Url='"+url+"', transedContent='"+tmpht+"' where mbloginfoId='"+id+"';";
                            stat3.execute(q3);
                            stat3.close();
                        }catch(Exception e)
                        {}
                }

            }
            rs.close();
            stat4.close();
            pos = pos +10000;
        }
        
        stat1.close();
        System.out.println("Over!!!!!!!");
        
    }


}


