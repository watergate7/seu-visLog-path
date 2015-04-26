package entity;


import com.google.gson.Gson;

import java.net.URL;
import java.util.*;

/**
 * Created by Administrator on 2015/3/16.
 */
public class SankeyGraph extends Graph{
//    ArrayList<URLNode> nodes;
//    ArrayList<StreamEdge> links;

    public SankeyGraph(List<URLNode> nodes, List<StreamEdge> links){
        super(nodes,links);
    }

//    public SankeyGraph(List<URLNode> nodes, List<StreamEdge> links){
//        this.nodes=(ArrayList<URLNode>)nodes;
//        this.links=(ArrayList<StreamEdge>)links;
//    }

    public SankeyGraph FilterByEdgeVol(double filter){
        //按边的value值对需要显示的数据进行过滤
        Set<Integer> nodeset=new HashSet<Integer>();
        Iterator<? extends Edge> iter_e=this.links.iterator();
        while(iter_e.hasNext()){
            Edge edge=iter_e.next();
            double value=((StreamEdge)edge).getValue();
            if(value<filter)
                iter_e.remove();
            else{
                nodeset.add(edge.getSource());
                nodeset.add(edge.getTarget());
            }
        }
        Iterator<? extends Node> iter_n=this.nodes.iterator();
        while(iter_n.hasNext()){
            Node node=iter_n.next();
            if(!nodeset.contains(node.getName())){
                iter_n.remove();
            }
        }

        //将过滤过后的json结果再次按node的name从0开始排好
        int index=0;
        for(Node n:this.nodes){
            int old_name=n.getName();
            int new_name=index++;
            n.setName(new_name);
            for(Edge e:this.links){
                if(e.getSource()==old_name)
                    e.setSource(new_name);
                if(e.getTarget()==old_name)
                    e.setTarget(new_name);
            }
        }
        return this;
    }

    public List<URLNode> get_mainland(int k){
        List<URLNode> mainland_nodes=new ArrayList<URLNode>();
        List<? extends Node> nodes=this.getNodes();
        for(Node node:nodes){
            URLNode n=(URLNode)node;
            if(n.getDepth()==0){
//                System.out.println(n.getIn_degree());
                for(URLNode no:mainland_nodes){
//                    System.out.print(no.getIn_degree()+"  ");
                }
                if(mainland_nodes.size()<k){
                    mainland_nodes.add(n);
                }
                else{
                    URLNode minnode=getMinNode(mainland_nodes,"in_degree");
//                    System.out.print("[" + minnode.getIn_degree() + "]");
                    if(minnode.getIn_degree()<n.getIn_degree()){
                        mainland_nodes.remove(minnode);
                        mainland_nodes.add(n);
                    }
                }
//                System.out.println();
            }
        }
        return mainland_nodes;
    }

    public List<URLNode> get_highdrop(int k,double threshold){
        List<URLNode> highdrop_nodes=new ArrayList<URLNode>();
        List<? extends Node> nodes=this.getNodes();
        for(Node node:nodes){
            URLNode n=(URLNode)node;
            if(n.getIn_degree()>threshold){
                if(highdrop_nodes.size()<k){
                    highdrop_nodes.add(n);
                }
                else{
                    URLNode minnode=getMinNode(highdrop_nodes,"drop_percent");
                    if(minnode.getDrop_per()<n.getDrop_per()){
                        highdrop_nodes.remove(minnode);
                        highdrop_nodes.add(n);
                    }
                }
            }
        }
        return highdrop_nodes;
    }

    public URLNode getMinNode(List<URLNode> nodes,String param){
        URLNode node=nodes.get(0);
        for(int i=1;i<nodes.size();i++){
            double quota1=0;
            double quota2=0;
            if(param.equals("in_degree")){
                quota1=node.getIn_degree();
                quota2=nodes.get(i).getIn_degree();
            }
            if(param.equals("drop_percent")){
                quota1=node.getDrop_per();
                quota2=nodes.get(i).getDrop_per();
            }
            if(quota1>quota2)
                node=nodes.get(i);
        }
        return node;
    }
}
