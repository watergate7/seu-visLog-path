package entity;

/**
 * Created by Administrator on 2015/3/16.
 */
public class URLNode extends Node {
    protected String url_index;
    protected String url;
    protected String semantics;
    protected double out_degree;
    protected double in_degree;
    protected int drop;
    protected int land;
    protected double drop_per;
    protected String datetime;
    protected int depth;

    public double getDrop_per() {
        return drop_per;
    }

    public void setDrop_per(double drop_per) {
        this.drop_per = drop_per;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
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

    public void setIn_degree(double in){
        in_degree=in;
    }

    public void setOut_degree(double out){
        out_degree=out;
    }

    public void setUrl(String url){
        this.url=url;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }


    public double getIn_degree() {
        return in_degree;
    }

    public double getOut_degree() {
        return out_degree;
    }
}
