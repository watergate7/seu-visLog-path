package entity;

/**
 * Created by Administrator on 2015/3/16.
 */
public class Edge {
    protected int source;
    protected int target;

    public Edge(int source,int target){
        this.source=source;
        this.target=target;
    }

    public int getSource(){
        return source;
    }

    public int getTarget(){
        return target;
    }
}
