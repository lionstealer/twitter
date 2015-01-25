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
public class Check {

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

    
    public static void checkfollowing() throws SQLException
    {
        Connection conn=conpool.getConnection();
        
        int flag=0;
        Timestamp ts=Timestamp.valueOf("2013-09-05 10:19:30");
        long time=ts.getTime();
        
        while(true)
        {
            long du=0;
            
            Statement stat1=conn.createStatement();
            String q1="select userId from tb_sample_users_manip where isUpdate=1 and ischecked=0 limit 1;";
            ResultSet rs1=stat1.executeQuery(q1);
            rs1.next();
            long uid=rs1.getLong(1);
            rs1.close();
            stat1.close();
            
            System.out.println(uid);
            
            Statement stat2=conn.createStatement();
            String q2="select friend from tb_sample_users_friend where user='"+uid+"';";
            ResultSet rs2=stat2.executeQuery(q2);
            while(rs2.next())
            {
                long fid=rs2.getLong(1);
                
                //check if fid exist in the tb_tmp
                Statement stat3=conn.createStatement();
                String q3="select id from tb_tmp where userId='"+fid+"';";
                ResultSet rs3=stat3.executeQuery(q3);
                if(rs3.next())
                {
                    flag=1;
                    System.out.println(flag+" "+fid);
                    
                    
//                    Statement stat6=conn.createStatement();
//                    String q6="select httime from tb_sample_dyninfo_old where httime<'2013-09-05 10:19:30' and userId='"+fid+"';";
//                    ResultSet rs6=stat6.executeQuery(q6);
//                    
//                    while(rs6.next())
//                    {
//                        Timestamp t=rs6.getTimestamp(1);
//                        long lt=t.getTime();
//                        long tmp=time-lt;
//                        if(du==0 || tmp<du)
//                        {
//                            du=tmp;
//                        }
//                        
//                    }
                    
                    Statement stat6=conn.createStatement();
                    String q6="insert into tb_friendtmp(userId) value('"+fid+"')";
                    stat6.execute(q6);
                    stat6.close();
                    
//                    rs3.close();
//                    stat3.close();
//                    break;
                }
                rs3.close();
                stat3.close();
            }
            
//            
//            
//            if(flag==1)
//            {
//                Statement stat4=conn.createStatement();
//                String q4="update tb_sample_users_manip set isfollowing=1 where userId='"+uid+"';";
//                stat4.execute(q4);
//                stat4.close();
//                
//                
//                Statement stat7=conn.createStatement();
//                String q7="update tb_sample_users_manip set duration='"+du+"' where userId='"+uid+"';";
//                stat7.execute(q7);
//                stat7.close();
//            }
            Statement stat5=conn.createStatement();
            String q5="update tb_sample_users_manip set ischecked=1 where userId='"+uid+"';";
            stat5.execute(q5);
            stat5.close();
            
            flag=0;
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
    
    public static void testUrls() throws IOException, SQLException
    {
        Connection conn = conpool.getConnection();
        String q1 = "select count(*) from tb_search_ht where Url is not null and Url <> '' and mbloginfoId>7720;";
        Statement stat1 = conn.createStatement();
        ResultSet rs1 = stat1.executeQuery(q1);
        rs1.next();
        int count = rs1.getInt(1);
        
        rs1.close();
        stat1.close();
        
        int pos=0;
        
        String q4="select position from tb_position where name='url';";
        Statement stat4 = conn.createStatement();
        ResultSet rs4 = stat4.executeQuery(q4);
        rs4.next();
        int fid = rs4.getInt(1);
        rs4.close();
        stat4.close();
        
        while(pos<count)
        {
            
            System.out.println("pos:"+pos);
            int limit = 10000;
            if(pos+10000>count)
            {
                limit = count-pos;
            }
            Statement stat2 = conn.createStatement();
            String query2 = "select mbloginfoId,Url from tb_search_ht where Url is not null and Url <> '' and mbloginfoid > '"+fid+"' limit "+limit+" offset "+pos+";";
            ResultSet rs2 = stat2.executeQuery(query2);
        
            //WebRuquest.getGetResponse(url, null, null,null);
            //AchieveWebContentUtil.getURLBodyBytes(Url, getCookie(),1);
            while(rs2.next())
            {
                int id = rs2.getInt(1);
                String turl = rs2.getString(2);
                
                System.out.println(id);
                
                try{

                    URL url = new URL(turl);
    //                HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    //                connection.setRequestMethod("GET");
    //                connection.connect();
    //
    //                int code = connection.getResponseCode();
    //                System.out.println(code);


                    //URL url = new URL("http://t.co/rVMDt0Rc");
                    URLConnection con = url.openConnection();
                    InputStream in = con.getInputStream();
                    String encoding = con.getContentEncoding();
                    encoding = encoding == null ? "UTF-8" : encoding;
                    //String body = IOUtils.toString(in, encoding);

                    InputStreamReader is = new InputStreamReader(in);
                    StringBuilder sb=new StringBuilder();
                    BufferedReader br = new BufferedReader(is);
                    String read = br.readLine();

                    while(read != null) {
                        //System.out.println(read);
                        sb.append(read);
                        read =br.readLine();

                    }

                    if(sb!=null)
                    {
                        String body=sb.toString();

                         if(body.contains("This link has been flagged as potentially harmful."))
                        {
                            System.out.println(id+": Suspended");

                            Statement stat3 = conn.createStatement();
                            String q3 = "update tb_search_ht set Urlsuspend = 1 where mbloginfoId = '"+id+"';";   
                            stat3.execute(q3);
                            stat3.close();
                        }
                     
                    }
                     
                    String q5="update tb_position set position='"+id+"' where name='url';";
                    Statement stat5 = conn.createStatement();
                    stat5.execute(q5);
                    stat5.close();
                    
                }catch(Exception e)
                {
                    System.out.println(e);
                }

                //File_Write("/Users/zhangyubao/Documents/tmp.txt",body);
               
                

            }
            rs2.close();
            stat2.close();
            
            pos=pos+10000;
        }
        return;
    }
       
     
}


