package dao;

import java.sql.*;

/**
 * Created by Administrator on 2015/4/1.
 */
public class URLDictDAO {
    private String URI;
    private String db;
    private Connection conn;
    private Statement statement;

    public URLDictDAO(String URI,String db){
        try{
            this.URI=URI;
            this.db=db;
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URI + "/" + db, "root", "million");
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

    public String[] getDictRowbyId(String id){
        String[] row=new String[2];
        try{
            String sql = "select * from dict where id='"+id+"';";
            ResultSet rs = statement.executeQuery(sql);
            if(rs.next()){
                String url = rs.getString(2);
                String semantics = rs.getString(3);
                row[0]=url;
                row[1]=semantics;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return row;
    }
}
