package demo;

import DataGenerator.SankeyDataGenerator;
import SqlTransformer.SankeySqltransformer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Administrator on 2015/3/16.
 */
public class SankeyDiagram{
    public static void main(String args[])throws IOException{
        String[] params={"ÍøÒ³","ÍøÒ³Á÷Á¿","2014-8-9","2014-8-11","6"};
        SankeySqltransformer sformer=new SankeySqltransformer(params);
        String result=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getGraph(sformer.EdgesSqltransform(),20);
        BufferedWriter bw=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\test.json"));
        System.out.println(result);
        bw.write(result);
        bw.close();
        String result1=new SankeyDataGenerator("jdbc:mysql://localhost:3306","log_stream").getCriteria(10);
        BufferedWriter bw1=new BufferedWriter(new FileWriter("C:\\Documents and Settings\\Administrator\\Desktop\\crti.json"));
        System.out.println(result1);
        bw1.write(result1);
        bw1.close();
    }
}
