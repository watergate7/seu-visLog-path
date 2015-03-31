package entity;

/**
 * Created by Administrator on 2015/3/16.
 */
public class StreamEdge extends Edge{
    protected int value;

    public StreamEdge(int source,int target,int value){
        super(source,target);
        this.value=value;
    }

    public int getValue(){
        return value;
    }
}
