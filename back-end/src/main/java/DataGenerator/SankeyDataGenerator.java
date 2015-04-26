package DataGenerator;

import com.google.gson.Gson;
import dao.StreamDAO;
import dao.URLDictDAO;
import entity.SankeyCriteriaJsonObj;
import entity.SankeyGraph;
import entity.StreamEdge;
import entity.URLNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by Administrator on 2015/3/15.
 */
public class SankeyDataGenerator extends BasicDataGenerator{
    protected Map<String,Integer> url_indegree;
    protected Map<String,Integer> url_outdegree;

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
    public SankeyGraph getGraph(String[] Edges_sqls,String[] Nodes_sqls){
        List<URLNode> nodes;
        //带session标记的路段数据，用于计算每个节点的out_degree和in_degree
        List<StreamEdge> links;
        //不带session标记的路段数据，带每条边的权值，用于显示
        List<StreamEdge> nosessionlinks;
        SankeyGraph graph=null;
        try {
            Statement statement = conn.createStatement();
            int node_index=0;
            nodes=new ArrayList<URLNode>();
            links=new ArrayList<StreamEdge>();
            nosessionlinks=new ArrayList<StreamEdge>();

            //两个map用于存放（起点->index），（终点->index）的映射
            Map<String,Integer> startmap;
            Map<String,Integer> endmap=null;
            //depthmap用于存放每个节点（用name标识）和在路径中深度的对应关系
            Map<Integer,Integer> depthmap=new HashMap<Integer, Integer>();

//            System.out.println(Arrays.toString(Edges_sqls));

            for(int i=0;i<Edges_sqls.length;i++){
                //存放不同session相同起点和终点的路段的ln(count)之和
                Map<String,Double> segCount=new HashMap<String, Double>();
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
//                    System.out.println(start_url);
                    String end_url = rs.getString(3);
                    String session = rs.getString(4);
//                    System.out.println(end_url);
                    if(!(start_url==null)&&!(end_url==null)){
                        if(i==0)
                            if(!startmap.containsKey(start_url)){
                                startmap.put(start_url,node_index);
                                depthmap.put(node_index,i);
                                node_index++;
                            }

                        if(!endmap.containsKey(end_url)){
                            endmap.put(end_url,node_index);
                            depthmap.put(node_index,i+1);
                            node_index++;
                        }


                        StreamEdge link=new StreamEdge(startmap.get(start_url),endmap.get(end_url),(double)count);
                        link.setSession(session);
                        links.add(link);

                        String key=start_url+"-"+end_url;
                        if(!segCount.containsKey(key)){
                            segCount.put(key,Math.log(count)+1);
                        }
                        else{
                           double old_count=segCount.get(key);
                            segCount.put(key,old_count+Math.log(count)+1);
                        }
                    }
                }

                for(Map.Entry<String,Double> entry:segCount.entrySet()){
                    String key=entry.getKey();
                    double count=entry.getValue();
                    String start_url=key.split("-")[0];
                    String end_url=key.split("-")[1];
                    StreamEdge nosessionlink=new StreamEdge(startmap.get(start_url),endmap.get(end_url),count);
                    nosessionlinks.add(nosessionlink);
                }

                if(i==0){
                    for(Map.Entry<String,Integer> entry:startmap.entrySet()){
                        URLNode node=new URLNode(entry.getValue(),entry.getKey());
                        node.setDepth(depthmap.get(entry.getValue()));
                        nodes.add(node);
                    }
                }
                for(Map.Entry<String,Integer> entry:endmap.entrySet()){
                    URLNode node=new URLNode(entry.getValue(),entry.getKey());
                    node.setDepth(depthmap.get(entry.getValue()));
                    nodes.add(node);
                }
            }
            //将nodes按照name排序
            Collections.sort(nodes);
            for(URLNode node:nodes){
                //两个map用于记录同一个session中以当前节点的为起点的边的value之和
                Map<String,Double> in_session=new HashMap<String, Double>();
                Map<String,Double> out_session=new HashMap<String, Double>();

                URLDictDAO dictDAO=new URLDictDAO("jdbc:mysql://localhost:3306","log_stream");
                //计算每个node的出度和入度
                double out=0;
                double in=0;
                for(StreamEdge link:links){
                    if(link.getSource()==node.getName()){
                        if(out_session.containsKey(link.getSession())){
                            double value=out_session.get(link.getSession());
                            out_session.put(link.getSession(),value+link.getValue());
                        }
                        else
                            out_session.put(link.getSession(),link.getValue());
                    }
                    if(link.getTarget()==node.getName()){
                        if(in_session.containsKey(link.getSession())){
                            double value=in_session.get(link.getSession());
                            in_session.put(link.getSession(),value+link.getValue());
                        }
                        else
                            in_session.put(link.getSession(),link.getValue());
                    }
                }
                for(Map.Entry<String,Double> entry:out_session.entrySet()){
                    out+=Math.log(entry.getValue())+1;
                }
                for(Map.Entry<String,Double> entry:in_session.entrySet()){
                    in+=Math.log(entry.getValue())+1;
                }
                node.setIn_degree(in);
                node.setOut_degree(out);
                //设置每个node的url和语义信息
                String[] row=dictDAO.getDictRowbyId(node.getUrl_index());
                node.setUrl(row[0]);
                node.setSemantics(row[1]);
            }
            //计算depth为0的节点的入度
            ResultSet rs = statement.executeQuery(Nodes_sqls[0]);
            Map<String,Double> Startout_session=new HashMap<String, Double>();

            while(rs.next()) {
                int count = rs.getInt(1);
                String start_url = rs.getString(2);
                String session = rs.getString(3);

                if(Startout_session.containsKey(start_url)){
                    double value=Startout_session.get(start_url);
                    Startout_session.put(start_url,value+Math.log((double)count)+1);
                }
                else
                    Startout_session.put(start_url,Math.log((double)count)+1);
            }
            for(URLNode node:nodes){
                if(node.getDepth()==0){
                    node.setIn_degree(Startout_session.get(node.getUrl_index()));
                }
            }
            //设置每个节点的跳出率
            for(URLNode node:nodes){
                double in=node.getIn_degree();
                double out=node.getOut_degree();
                node.setDrop_per((in-out)/in);
            }
            graph=new SankeyGraph(nodes,nosessionlinks);
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        return graph;
    }

//    public String getGraph(String[] Edges_sqls,int filter) {
//        Gson gson=new Gson();
//        String json_graph="";
//        List<URLNode> nodes;
//        List<StreamEdge> links;
//        SankeyGraph graph;
//        try {
//            Statement statement = conn.createStatement();
//            int node_index=0;
//            nodes=new ArrayList<URLNode>();
//            links=new ArrayList<StreamEdge>();
//
//            //两个map用于存放（起点->index），（终点->index）的映射
//            Map<String,Integer> startmap;
//            Map<String,Integer> endmap=null;
//
////            System.out.println(Arrays.toString(Edges_sqls));
//
//            for(int i=0;i<Edges_sqls.length;i++){
//                //初始化map
//                if(i==0){
//                    startmap=new HashMap<String,Integer>();
//                }
//                else{
//                    startmap=endmap;
//                }
//                endmap=new HashMap<String,Integer>();
//
//                String sql=Edges_sqls[i];
//                ResultSet rs = statement.executeQuery(sql);
//
//                while(rs.next()) {
//                    int count = rs.getInt(1);
//                    String start_url = rs.getString(2);
////                    System.out.println(start_url);
//                    String end_url = rs.getString(3);
////                    System.out.println(end_url);
//                    if(count<filter)
//                        continue;
//                    if(!(start_url==null)&&!(end_url==null)){
//                        if(i==0)
//                            if(!startmap.containsKey(start_url))
//                                startmap.put(start_url,node_index++);
//                        if(!endmap.containsKey(end_url))
//                            endmap.put(end_url,node_index++);
//                        StreamEdge link=new StreamEdge(startmap.get(start_url),endmap.get(end_url),count);
//                        links.add(link);
//                    }
//                }
//                if(i==0){
//                    for(Map.Entry<String,Integer> entry:startmap.entrySet()){
//                        URLNode node=new URLNode(entry.getValue(),entry.getKey());
//                        nodes.add(node);
//                    }
//                }
//                for(Map.Entry<String,Integer> entry:endmap.entrySet()){
//                    URLNode node=new URLNode(entry.getValue(),entry.getKey());
//                    nodes.add(node);
//                }
//            }
//            //将nodes按照name排序
//            Collections.sort(nodes);
//            for(URLNode node:nodes){
//                URLDictDAO dictDAO=new URLDictDAO("jdbc:mysql://localhost:3306","log_stream");
//                //计算每个node的出度和入度
//                int out=0;
//                int in=0;
//                for(StreamEdge link:links){
//                    if(link.getSource()==node.getName())
//                        out+=link.getValue();
//                    if(link.getTarget()==node.getName())
//                        in+=link.getValue();
//                }
//                node.setIn_degree(in);
//                node.setOut_degree(out);
//                //设置每个node的url和语义信息
//                String[] row=dictDAO.getDictRowbyId(node.getUrl_index());
//                node.setUrl(row[0]);
//                node.setSemantics(row[1]);
//            }
//            graph=new SankeyGraph(nodes,links);
//            json_graph=gson.toJson(graph);
//        }
//        catch(SQLException e){
//            e.printStackTrace();
//        }
//        return json_graph;
//    }



}
