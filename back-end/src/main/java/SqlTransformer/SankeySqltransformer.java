package SqlTransformer;

/**
 * Created by Administrator on 2015/3/15.
 */
public class SankeySqltransformer extends BasicSqltransformer {

    public SankeySqltransformer(String[] Params){
        super(Params);
    }

    @Override
    public String[] NodesSqltransform() {
        int num=6;
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String sql="select distinct P"+i+" from streams_str;";
            sqls[i-start_P]=sql;
        }
        return sqls;
    }

    @Override
    public String[] EdgesSqltransform() {
        int num=5;
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String start="P"+i;
            String end="P"+(i+1);
            String sql="select count(*),"+start+","+end+" from streams_str group by "+start+","+end;
            sqls[i-start_P]=sql;
        }
        return sqls;
    }
}
