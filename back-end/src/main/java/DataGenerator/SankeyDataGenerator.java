package DataGenerator;

import com.google.gson.Gson;
import dao.StreamDAO;
import dao.URLDictDAO;
import entity.SankeyCriteria;
import entity.SankeyGraph;
import entity.StreamEdge;
import entity.URLNode;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by Administrator on 2015/3/15.
 */
public class SankeyDataGenerator extends BasicDataGenerator{
    public SankeyDataGenerator(String db_uri,String db_name){
        super(db_uri,db_name);
    }
    @Override
    public String getEdges(String[] sqls) {
        return null;
    }

    @Override
    public String getNodes(String[] sqls) {
        return null;
    }

    @Override
    public String getGraph(String[] Edges_sqls,int filter) {
        Gson gson=new Gson();
        String json_graph="";
        List<URLNode> nodes;
        List<StreamEdge> links;
        SankeyGraph graph;
        try {
            Statement statement = conn.createStatement();
            int node_index=0;
            nodes=new ArrayList<URLNode>();
            links=new ArrayList<StreamEdge>();

            //两个map用于存放（起点->index），（终点->index）的映射
            Map<String,Integer> startmap;
            Map<String,Integer> endmap=null;

//            System.out.println(Arrays.toString(Edges_sqls));

            for(int i=0;i<Edges_sqls.length;i++){
                //初始化map
                if(i==0){
                    startmap=new HashMap<String,Integer>();
                }
                else{
                    startmap=endmap;
                }
                endmap=new HashMap<String,Integer>();

                String sql=Edges_sqls[i];
                ResultSet rs = statement.executeQuery(sql);

                while(rs.next()) {
                    int count = rs.getInt(1);
                    String start_url = rs.getString(2);
                    String end_url = rs.getString(3);
                    if(count<filter)
                        continue;
                    if(!start_url.equals("nul")&&!end_url.equals("nul")){
                        if(i==0)
                            if(!startmap.containsKey(start_url))
                                startmap.put(start_url,node_index++);
                        if(!endmap.containsKey(end_url))
                            endmap.put(end_url,node_index++);
                        StreamEdge link=new StreamEdge(startmap.get(start_url),endmap.get(end_url),count);
                        links.add(link);
                    }
                }
                if(i==0){
                    for(Map.Entry<String,Integer> entry:startmap.entrySet()){
                        URLNode node=new URLNode(entry.getValue(),entry.getKey());
                        nodes.add(node);
                    }
                }
                for(Map.Entry<String,Integer> entry:endmap.entrySet()){
                    URLNode node=new URLNode(entry.getValue(),entry.getKey());
                    nodes.add(node);
                }
            }
            //将nodes按照name排序
            Collections.sort(nodes);
            for(URLNode node:nodes){
                URLDictDAO dictDAO=new URLDictDAO("jdbc:mysql://localhost:3306","log_stream");
                //计算每个node的出度和入度
                int out=0;
                int in=0;
                for(StreamEdge link:links){
                    if(link.getSource()==node.getName())
                        out+=link.getValue();
                    if(link.getTarget()==node.getName())
                        in+=link.getValue();
                }
                node.setIn_degree(in);
                node.setOut_degree(out);
                //设置每个node的url和语义信息
                String[] row=dictDAO.getDictRowbyId(node.getUrl_index());
                node.setUrl(row[0]);
                node.setSemantics(row[1]);
            }
            graph=new SankeyGraph(nodes,links);
            json_graph=gson.toJson(graph);
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        return json_graph;
    }

    public String getCriteria(int topk){
        Gson gson=new Gson();
        String crit="";
        StreamDAO strDAO=new StreamDAO("jdbc:mysql://localhost:3306","log_stream");
        List<String> topkdrop=strDAO.getTopkDrop(topk);
        List<String> topkland=strDAO.getTopkLand(topk);
        List<URLNode> highdrop=new ArrayList<URLNode>();
        List<URLNode> mainland=new ArrayList<URLNode>();

        for(String str:topkdrop){
            String url=str.split(",")[0];
            int count=Integer.parseInt(str.split(",")[1]);
            URLNode node=new URLNode();
            node.setUrl(url);
            node.setDrop(count);
            highdrop.add(node);
        }

        for(String str:topkland){
            String url=str.split(",")[0];
            int count=Integer.parseInt(str.split(",")[1]);
            URLNode node=new URLNode();
            node.setUrl(url);
            node.setLand(count);
            mainland.add(node);
        }
        crit=gson.toJson(new SankeyCriteria(highdrop,mainland));
        return crit;
    }
}
