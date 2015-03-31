package entity;

/**
 * Created by Administrator on 2015/3/16.
 */
public class URLNode extends Node {
    protected String url;
    protected int out_degree;
    protected int in_degree;

    public URLNode(Integer name,String url){
        super(name);
        this.url=url;
    }

    public void setIn_degree(int in){
        in_degree=in;
    }

    public void setOut_degree(int out){
        out_degree=out;
    }
}
