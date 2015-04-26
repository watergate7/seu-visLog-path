package DataGenerator;

import entity.Graph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Administrator on 2015/3/15.
 */
public abstract class BasicDataGenerator {
    protected Connection conn;
    public BasicDataGenerator(String db_uri,String db_name){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(db_uri + "/" + db_name, "root", "million");
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    protected void disCon(){
        try{
            conn.close();
        }
        catch(SQLException e){
            e.printStackTrace();
        }
    }

    public abstract String getNodes(String[] sqls);
    public abstract String getEdges(String[] sqls);
    public abstract Graph getGraph(String[] Edges_sqls,String[] Nodes_sqls);
}
