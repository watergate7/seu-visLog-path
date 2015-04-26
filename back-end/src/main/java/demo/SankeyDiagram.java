package demo;

import DataGenerator.SankeyDataGenerator;
import SqlTransformer.SankeySqltransformer;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import dao.LogDAO;
import entity.SankeyGraph;
import entity.SankeyGraphJsonObj;
import entity.URLNode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by Administrator on 2015/3/16.
 */
public class SankeyDiagram {
    public static void main(String args[]) throws IOException {
        //获取群体访问行为路径
//        String[] params={"网页","网页流量","2014-8-9","2014-8-11","6"};
//        SankeySqltransformer sformer=new SankeySqltransformer(params);
//        String result=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getGraph(sformer.EdgesSqltransform(),20);
//        BufferedWriter bw=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\test.json"));
//        System.out.println(result);
//        bw.write(result);
//        bw.close();
        //获取TOP10的着陆页和跳离页
//        String result1=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getCriteria(10);
//        BufferedWriter bw1=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\crti.json"));
//        System.out.println(result1);
//        bw1.write(result1);
//        bw1.close();
//        获取单个session的点击流路径
//        String[] params2={"网页","网页流量","2014-8-9","2014-8-11","6"};
//        SankeySqltransformer sformer2=new SankeySqltransformer(params2);
//        String result2=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getGraph(sformer2.PathsSqltransformBySessionID("DEuMjQ5LjE0NC4yMzkyMDE0MDgwOTIzNTYzOTUzODk2Nzk3NTAyN"),0);
//        BufferedWriter bw2=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\paths.json"));
//        System.out.println(result2);
//        bw2.write(result2);
//        bw2.close();
//        获取单个session的访问跳转顺序
//        LogDAO logDAO = new LogDAO("mongodb://223.3.75.101:27017","jiaodian");
//        String path=logDAO.getPathBySession("DEuMjQ5LjE0NC4yMzkyMDE0MDgwOTIzNTYzOTUzODk2Nzk3NTAyN");
//        BufferedWriter bw3=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\sequence.json"));
//        bw3.write(path);
//        bw3.close();
//        System.out.println(path);

        //获取群体访问行为路径
        long beg=System.currentTimeMillis();
        String[] params={"2014-8-9","2014-8-11","6"};
        SankeySqltransformer sformer=new SankeySqltransformer(params);
        SankeyGraph sankeyGraph=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getGraph(sformer.EdgesSqltransform(),sformer.NodesSqltransform());
        List<URLNode> highdrop=sankeyGraph.get_highdrop(10,20);
        List<URLNode> mainland=sankeyGraph.get_mainland(10);
        String result=new SankeyGraphJsonObj(sankeyGraph.FilterByEdgeVol(10),highdrop,mainland).toJson();
//        String result=new SankeyGraphJsonObj(sankeyGraph).toJson();
        long end=System.currentTimeMillis();
        System.out.println("Last:"+(end-beg));
        BufferedWriter bw=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\graph2.json"));
        System.out.println(result);
        bw.write(result);
        bw.close();
    }
}
