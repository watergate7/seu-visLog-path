package dao;

import PathCal.Log2Stream;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by Administrator on 2015/3/14.
 */
public class StreamDAO {
    private String URI;
    private String db;
    private Connection conn;
    private DictDAO dict;
    private PreparedStatement pstmt;
    private Statement statement;

    public StreamDAO(String URI,String db,DictDAO dict){
        try{
            this.URI=URI;
            this.db=db;
            this.dict=dict;
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URI+"/"+ db, "root", "million");
            statement = conn.createStatement();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    public StreamDAO(String URI,String db){
        try{
            this.URI=URI;
            this.db=db;
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URI+"/"+ db, "root", "million");
            statement = conn.createStatement();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void finalize(){
        try{
            statement.close();
            conn.close();
        }
        catch(SQLException e){
            e.printStackTrace();
        }
    }

    //每个session的每个stream写一次数据库，效率低
//    public boolean insertStreams(List<ArrayList<String[]>> Streams,String session){
//        try{
//            Statement stmt=conn.createStatement();
//            String insert;
//            for(List<String[]> stream:Streams){
//                insert="insert into streams_str values (";
//                for(String[] point:stream){
//                    String value=point[Log2Stream.url].equals("nul")?"nul":dict.getUrlIndex(point[Log2Stream.url]).toString();
//                    insert+="'"+value+"',";
//                }
//                insert+="'"+session+"');";
//                System.out.println(insert);
//                stmt.executeUpdate(insert);
//            }
//        }
//        catch(Exception e){
//            e.printStackTrace();
//            return false;
//        }
//        return true;
//    }

    //每个session写一次，有空再改成一个batch写一次
    public boolean insertStreams(List<ArrayList<String[]>> Streams,String session){
        try{
            String sql = "insert into streams values (?,?,?,?,?,?,?,?,?,?,?,?);";
            pstmt = conn.prepareStatement(sql);

            for(List<String[]> stream:Streams){
                for(int i=0;i<stream.size();i++){
                    String[] point=stream.get(i);
//                    String value=point[Log2Stream.url];
                    String value=point[Log2Stream.url].equals("nul")?"nul":dict.getUrlIndex(point[Log2Stream.url]).toString();
                    pstmt.setString(i+1,value);
                }
                pstmt.setString(12,session);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public List<String> getTopkDrop(int k){
        List<String> topk=new ArrayList<String>();
        try{
            String sql = "select count(*) as c,P_end,dict.url from streams,dict where streams.P_end=dict.id group by P_end order by c desc limit "+k+";";
            ResultSet rs = statement.executeQuery(sql);
            while(rs.next()) {
                int count=rs.getInt(1);
                String url=rs.getString(3);
                String str=url+","+String.valueOf(count);
                topk.add(str);
            }
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        return topk;
    }

    public List<String> getTopkLand(int k){
        List<String> topk1=new ArrayList<String>();
        List<String> topk2=new ArrayList<String>();
        List<String> topk=new ArrayList<String>();
        try{
            String sql1 = "select count(*) c,P1,dict.url from streams,dict where P1=dict.id and dict.url like" +
                    " '%made-in-china%' group by P1 order by c desc limit " + k +";";
            ResultSet rs1 = statement.executeQuery(sql1);
            while(rs1.next()) {
                int count=rs1.getInt(1);
                String url=rs1.getString(3);
                String str=url+","+String.valueOf(count);
                topk1.add(str);
            }

            String sql2 = "select count(*) c,d2.url,d1.url " +
                    "from streams s join dict d2 " +
                    "on s.P2=d2.id " +
                    "left join dict d1 " +
                    "on s.P1=d1.id " +
                    "where d1.url not" +
                    " like '%made-in-china%' group by s.P2 order by c desc limit "+k+";";
            ResultSet rs2 = statement.executeQuery(sql2);
            while(rs2.next()) {
                int count=rs2.getInt(1);
                String url=rs2.getString(2);
                String str=url+","+String.valueOf(count);
                topk2.add(str);
            }
            int i=10;
            int cursor1=0;
            int cursor2=0;
            while(i-->0){
                int k1=Integer.parseInt(topk1.get(cursor1).split(",")[1]);
                int k2=Integer.parseInt(topk2.get(cursor2).split(",")[1]);
                if(k1>=k2){
                    topk.add(topk1.get(cursor1));
                    cursor1++;
                }
                else{
                    topk.add(topk2.get(cursor2));
                    cursor2++;
                }
            }
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        return topk;
    }
}
