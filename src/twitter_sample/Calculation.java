/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitter_sample;

//import weiboprocess.ConnectionPool;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.mysql.jdbc.Connection;

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
public class Calculation {

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
   
    public static void caltrend() throws SQLException
    {
        Connection conn=(Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");
        
        //coverage and ranking of sample trends and public trends
        while(true)
        {
            Statement stat1=conn.createStatement();
            String q1="select processpos from tb_position where name='trendtime';";
            ResultSet rs1=stat1.executeQuery(q1);
            rs1.next();
            int pos=rs1.getInt(1);
            rs1.close();
            stat1.close();
            
            Timestamp trendtime=null;
            Timestamp sampletime=null;
            String[] pubtrend=new String[10];
            String[] samtrend=new String[15];
            Statement stat2=conn.createStatement();
            String q2="select id,t_name,t_inserttime from tb_trend where t_type='Trend' and id>'"+pos+"' limit 10;";
            ResultSet rs2=stat2.executeQuery(q2);
            int i=0;
            while(rs2.next())
            {
                int id=rs2.getInt(1);
                
                Statement stat21=conn.createStatement();
                String q21="update tb_position set processpos='"+id+"' where name='trendtime';";
                stat21.execute(q21);
                stat21.close();
                
                String name=rs2.getString(2);
                trendtime=rs2.getTimestamp(3);
                pubtrend[i]=name.toUpperCase();
                i++;
            }
            rs2.close();
            stat2.close();
            
            
            Statement stat3=conn.createStatement();
            String q3="select id,t_name,t_inserttime from tb_trend where t_type like 'Sample%' and id>'"+pos+"' limit 15;";
            ResultSet rs3=stat3.executeQuery(q3);
            int j=0;
            while(rs3.next())
            {
                int id=rs3.getInt(1);
                String name=rs3.getString(2);
                sampletime=rs3.getTimestamp(3);
                
                if(Math.abs(trendtime.getTime()-sampletime.getTime())<=30*60*1000)
                {
                    Statement stat31=conn.createStatement();
                    String q31="update tb_position set processpos='"+id+"' where name='trendtime';";
                    stat31.execute(q31);
                    stat31.close();
                }else{
                    break;
                }
                
                samtrend[j]=name.toUpperCase();
                j++;
            }
            rs3.close();
            stat3.close();
            
            if(Math.abs(trendtime.getTime()-sampletime.getTime())>30*60*1000)
            {
                continue;
            }
            
            //calculate coverage
            int coverage=0;
            int position=0;
            for(int k=0;k<pubtrend.length;k++)
            {
                for(int l=0;l<samtrend.length;l++)
                {
                    if(pubtrend[k].equals(samtrend[l]))
                    {
                        coverage++;
                        position=position+(l+1);
                    }
                }
            }
            double meanpos=0;
            if(coverage>0)
            {
                meanpos = (double)position/coverage;
            }
            Statement stat4=conn.createStatement();
            String q4="insert tb_trend_coverage(coverage,meanpos,trendtime)values('"+coverage+"','"+meanpos+"','"+trendtime+"')";
            stat4.execute(q4);
            stat4.close();
            
        }
    }
    
    public static void movesharpfromtrend() throws SQLException
    {
        Connection conn=(Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");
        Statement stat1=conn.createStatement();
        String q1="select id,t_name from tb_trend where t_type='Trend' and t_name like '#%';";
        ResultSet rs1=stat1.executeQuery(q1);
        while(rs1.next())
        {
            int id=rs1.getInt(1);
            String name=rs1.getString(2);
            name=name.substring(1, name.length());
            Statement stat2=conn.createStatement();
            String q2="update tb_trend set t_name='"+name+"' where id='"+id+"';";
            stat2.execute(q2);
            stat2.close();
        }
        rs1.close();
        stat1.close();
        System.out.println("over!!!");
    }
    
    public static void calsampledynamics() throws SQLException, FileNotFoundException, IOException
    {
        Connection conn=(Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");

        while(true)
        {
            
            Statement stat0=conn.createStatement();
            String q0="select processpos from tb_position where name='sampleht';";
            ResultSet rs0=stat0.executeQuery(q0);
            rs0.next();
            int ht=rs0.getInt(1);
            rs0.close();
            stat0.close();

            Statement stat1=conn.createStatement();
            String q1="select hashtag from tb_sampleht where id='"+ht+"';";
            ResultSet rs1=stat1.executeQuery(q1);
            rs1.next();
            String topic=rs1.getString(1);
            rs1.close();
            stat1.close();

            Statement stat01=conn.createStatement();
            String q01="select position from tb_position where name='samplehttime';";
            ResultSet rs01=stat01.executeQuery(q01);
            rs01.next();
            int maxhttime=rs01.getInt(1);
            rs01.close();
            stat01.close();

            Statement stat02=conn.createStatement();
            String q02="select processpos from tb_position where name='samplehttime';";
            ResultSet rs02=stat02.executeQuery(q02);
            rs02.next();
            int httime=rs02.getInt(1);
            rs02.close();
            stat02.close();

            while(httime<=maxhttime)
            {

                ////////////////get time span///////////////
                Statement stat2=conn.createStatement();
                String q2="select stime from tb_sample_time where id>='"+httime+"' limit 2;";
                ResultSet rs2=stat2.executeQuery(q2);
                rs2.next();
                Timestamp starttime=rs2.getTimestamp(1);
                rs2.next();
                Timestamp endtime=rs2.getTimestamp(1);
                rs2.close();
                stat2.close();
                
                httime=httime+1;
                
                    //get dynamics of sample data set from starttime to endtime
//                    Statement stat4=conn.createStatement();
//                    String q4="select count(hashtag) from tb_netrel where "
//                            + "timeinfo>'"+starttime+"' and timeinfo<='"+endtime+"' and hashtag='"+topic+"';";
//                    ResultSet rs4=stat4.executeQuery(q4);
//                    rs4.next();
//                    int cnt=rs4.getInt(1);
//                    rs4.close();
//                    stat4.close();

                    //get dynamics of sample data set from starttime to endtime
                    Statement stat4=conn.createStatement();
                    String q4="select count(hashtag) from tb_sample_dyninfo where "
                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"';";
                    ResultSet rs4=stat4.executeQuery(q4);
                    rs4.next();
                    int cnt=rs4.getInt(1);
                    rs4.close();
                    stat4.close();

                    //get user number
                    Statement stat5=conn.createStatement();
                    String q5="select count(distinct userId) from tb_sample_dyninfo where "
                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"';";
                    ResultSet rs5=stat5.executeQuery(q5);
                    rs5.next();
                    int usercnt=rs5.getInt(1);
                    rs5.close();
                    stat5.close();


                    //get rt number
//                    Statement stat6=conn.createStatement();
//                    String q6="select count(*) from tb_sample_dyninfo where "
//                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"' and rt=1;";
//                    ResultSet rs6=stat6.executeQuery(q6);
//                    rs6.next();
//                    int rtcnt=rs6.getInt(1);
//                    rs6.close();
//                    stat6.close();

                    //get mt number
                    Statement stat7=conn.createStatement();
                    String q7="select count(*) from tb_sample_dyninfo where "
                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"' and mt<>0;";
                    ResultSet rs7=stat7.executeQuery(q7);
                    rs7.next();
                    int mtcnt=rs7.getInt(1);
                    rs7.close();
                    stat7.close();

                    //get follow count
                    int followcnt=0;
                    Statement stat8=conn.createStatement();
                    String q8="select followcount from tb_sample_dyninfo where "
                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"';";
                    ResultSet rs8=stat8.executeQuery(q8);
                    while(rs8.next())
                    {
                        int fcnt=rs8.getInt(1);
                        followcnt=followcnt+fcnt;
                    }

                    rs8.close();
                    stat8.close();

                    //get tweet count
                    int tweetcnt=0;
                    Statement stat9=conn.createStatement();
                    String q9="select tweetcount from tb_sample_dyninfo where "
                            + "httime>'"+starttime+"' and httime<='"+endtime+"' and hashtag='"+topic+"';";
                    ResultSet rs9=stat9.executeQuery(q9);
                    while(rs9.next())
                    {
                        int tcnt=rs9.getInt(1);
                        tweetcnt=tweetcnt+tcnt;
                    }

                    rs8.close();
                    stat8.close();


                    //insert topic
                    int rtcnt=0;
                    Statement stat_in=conn.createStatement();
                    String q_in="insert into tb_sample_topicdyn(hashtag,frequency,timeinfo,usernumber,rtnumber,mtnumber,"
                            + "followcount,tweetcount)"
                            + " values ('"+topic+"','"+cnt+"','"+starttime+"','"+usercnt+"','"+rtcnt+"','"+mtcnt+"',"
                            + "'"+followcnt+"','"+tweetcnt+"')";
                    stat_in.execute(q_in);
                    stat_in.close();
                    
//                    Statement stat_in=conn.createStatement();
//                    String q_in="insert into tb_netrel_dyn(ht,relnum,timeinfo)"
//                            + " values ('"+topic+"','"+cnt+"','"+starttime+"')";
//                    stat_in.execute(q_in);
//                    stat_in.close();
                    
                    //renew position
                    Statement stat3=conn.createStatement();
                    String q3="update tb_position set processpos='"+httime+"' where name='samplehttime';";
                    stat3.execute(q3);
                    stat3.close();


                    Timestamp t=new Timestamp(System.currentTimeMillis());
                    System.out.println("["+t+"]:"+ht+"th topic and "+httime+"th time...");
            }//while
            
            Statement stat00=conn.createStatement();
            String q00="update tb_position set processpos='"+(ht+1)+"' where name='sampleht';";
            stat00.execute(q00);
            stat00.close();
            
            //renew position
            Statement stat20=conn.createStatement();
            String q20="update tb_position set processpos=1 where name='samplehttime';";
            stat20.execute(q20);
            stat20.close();
            
            
        }//while

           
         //conpool.returnConnection(conn);
        
            
        
    }
   
    public static void extractcascade() throws SQLException
    {
        Connection conn = (Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");
        
                Statement stat3 = conn.createStatement();
        String query3 = "select count(mbloginfoId) "
                +"from tb_search where hashtag='teamfollowback' "
//                    +"from tb_search where hashtag='ipad' or hashtag='iphonegames' or hashtag='10TurnOns' or "
//                    + "hashtag='android' or hashtag='gameinsight' or hashtag='androidgames' or hashtag='ipadgames';";
//                    + "from tb_search_ht where (hashtag='Follow' or hashtag='SOUGOFOLLOW' or hashtag='followme' or "
//                    + "hashtag='TeamFollowBack' or hashtag='TEAMFOLLOWBACK' or hashtag='Followback' or "
//                    + "hashtag='sougofollow' or hashtag='FollowMe' or hashtag='ff' or hashtag='500ADAY' or "
//                    + "hashtag='teamfollowback' or hashtag='FollowME' or hashtag='OPENFOLLOW' or hashtag='followback' or "
//                    + "hashtag='TFBJP' or hashtag='Retweet' or hashtag='500aDay' or hashtag='FF' or hashtag='FollowBackSeguro' "
//                    + "or hashtag='OpenFollow' or hashtag='followmejp' or hashtag='openfollow' or hashtag='F4F' or hashtag='AutoFollowBack' or "
//                    + "hashtag='RT2GAIN' or hashtag='FOLLOWBACK' or hashtag='FOLLOW' or hashtag='TFB' or hashtag='TEAMHITFOLLOW' or "
//                    + "hashtag='RETWEET' or hashtag='90sBabyFollowTrain') "
                            + "and content like '%RT @%';";
//        
                    ResultSet rs3 = stat3.executeQuery(query3);
            rs3.next();
            int count2 = rs3.getInt(1);
            rs3.close();
            stat3.close();
            
            int pos2=0;

        
        while(pos2<count2)
        {
            System.out.println("search:"+pos2);
            int limit = 10000;
            if(pos2+10000>count2)
            {
                limit = count2-pos2;
            }
        
        
            Statement stat1=conn.createStatement();
            String q1="select userId,content,publishTime from tb_search_ht where hashtag='teamfollowback' "
//                    + "(hashtag='Follow' or hashtag='SOUGOFOLLOW' or hashtag='followme' or "
//                    + "hashtag='TeamFollowBack' or hashtag='TEAMFOLLOWBACK' or hashtag='Followback' or "
//                    + "hashtag='sougofollow' or hashtag='FollowMe' or hashtag='ff' or hashtag='500ADAY' or "
//                    + "hashtag='teamfollowback' or hashtag='FollowME' or hashtag='OPENFOLLOW' or hashtag='followback' or "
//                    + "hashtag='TFBJP' or hashtag='Retweet' or hashtag='500aDay' or hashtag='FF' or hashtag='FollowBackSeguro' "
//                    + "or hashtag='OpenFollow' or hashtag='followmejp' or hashtag='openfollow' or hashtag='F4F' or hashtag='AutoFollowBack' or "
//                    + "hashtag='RT2GAIN' or hashtag='FOLLOWBACK' or hashtag='FOLLOW' or hashtag='TFB' or hashtag='TEAMHITFOLLOW' or "
//                    + "hashtag='RETWEET' or hashtag='90sBabyFollowTrain') "
                    + "and content like '%RT @%' limit "+limit+" offset "+pos2+";";
            ResultSet rs1=stat1.executeQuery(q1);
            while(rs1.next())
            {
                String u1=rs1.getString(1);
                String u2=rs1.getString(2);
                String time=rs1.getString(3);

                u2=u2.substring(4, u2.length());
                String[] tmp=u2.split(":");
                u2=tmp[0];

                if(!u2.contains(" ") && !u2.contains("\n"))
                {
                    String s=u2+","+u1+","+time+"\n";

                    System.out.println(s);
                }
            }
            rs1.close();
            stat1.close();

            pos2 += 10000;
        }
        
        System.out.println("Over!");
        
        conpool.returnConnection(conn);
        
    }

    public static void calreciprocity() throws SQLException
    {
        Connection conn = (Connection) DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/twitter1",
				"root", "");
        Statement stat1=conn.createStatement();
        String q1="select count(usersrelationId) from tb_users_relation;";
        ResultSet rs1=stat1.executeQuery(q1);
        rs1.next();
        int totalnum = rs1.getInt(1);
        rs1.close();
        stat1.close();
        
        int count=0;
        while(count<totalnum)
        {
            Statement stat2 = conn.createStatement();
            String q2 = "select userId,followId,userLever from tb_users_relation limit 1 offset "+count+";";
            ResultSet rs2=stat2.executeQuery(q2);
            rs2.next();
            long uid=rs2.getInt(1);
            long fid=rs2.getInt(2);
            long uf=rs2.getInt(3);
            rs2.close();
            stat2.close();
            
            //Statement stat5=conn.createStatement();
            //String q5="select userId,followId,userLever from tb_users_relation where userlever='"+uf+"';";
//            ResultSet rs5 = stat5.executeQuery(q5);
//            while(rs5.next())
//            {
//                long uid1=rs2.getInt(1);
//                long fid1=rs2.getInt(2);
//                long uf1=rs2.getInt(3);
//                
//                
//            }
            
            try{
                Statement stat3 = conn.createStatement();
                String q3="select userId,followId,userLever from tb_users_relation where userlever='"+uf+"';";
                //String q3 = "select * from tb_users_relation where userId = '"+fid+"' and followId ='"+uid+"';";
                ResultSet rs3=stat3.executeQuery(q3);
                rs3.next();
                if(!rs3.isAfterLast()&&!rs3.isBeforeFirst())
                {
                    try{
                        Statement stat4=conn.createStatement();
                        String q4="insert into tb_user_reciprocity(userId,followId) values('"+uid+"','"+fid+"')";
                        stat4.execute(q4);
                        stat4.close();
                    }catch(Exception e)
                    {
                        
                    }
                }
                rs3.close();
                stat3.close();
            }catch(Exception e)
            {
                
            }
            
            count++;
            if(count%1000==0)
            {
                System.out.println(count+" -> "+totalnum);
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
     
}


