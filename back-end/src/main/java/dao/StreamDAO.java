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

    public StreamDAO(String URI,String db,DictDAO dict){
        try{
            this.URI=URI;
            this.db=db;
            this.dict=dict;
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URI+"/"+ db, "root", "million");
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
            String sql = "insert into streams_str values (?,?,?,?,?,?,?,?,?,?,?);";
            pstmt = conn.prepareStatement(sql);

            for(List<String[]> stream:Streams){
                for(int i=0;i<stream.size();i++){
                    String[] point=stream.get(i);
                    String value=point[Log2Stream.url];
//                    String value=point[Log2Stream.url].equals("nul")?"nul":dict.getUrlIndex(point[Log2Stream.url]).toString();
                    pstmt.setString(i+1,value);
                }
                pstmt.setString(11,session);
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
}
