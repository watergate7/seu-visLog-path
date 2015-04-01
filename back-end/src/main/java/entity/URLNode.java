package entity;

/**
 * Created by Administrator on 2015/3/16.
 */
public class URLNode extends Node {
    protected String url_index;
    protected String url;
    protected String semantics;
    protected int out_degree;
    protected int in_degree;
    protected int drop;
    protected int land;

    public void setLand(int land) {
        this.land = land;
    }

    public int getDrop() {
        return drop;
    }

    public int getLand() {
        return land;
    }

    public void setDrop(int drop) {
        this.drop = drop;

    }

    public URLNode(){}

    public URLNode(Integer name,String url_index){
        super(name);
        this.url_index=url_index;
    }

    public String getUrl_index(){
        return this.url_index;
    }

    public void setSemantics(String semantics){
        this.semantics=semantics;
    }

    public void setIn_degree(int in){
        in_degree=in;
    }

    public void setOut_degree(int out){
        out_degree=out;
    }

    public void setUrl(String url){
        this.url=url;
    }
}
