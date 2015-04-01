package SqlTransformer;

/**
 * Created by Administrator on 2015/3/15.
 */
public class SankeySqltransformer extends BasicSqltransformer {
    private final int start_Date=2;
    private final int end_Date=3;
    private final int path_len=4;

    public SankeySqltransformer(String[] Params){
        super(Params);
    }

    @Override
    public String[] NodesSqltransform() {
        int num=Integer.parseInt(Params[path_len]);
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String sql="select distinct P"+i+" from streams_new,session where streams_new.session=session.session and " +
                    "session.startTime>='"+Params[start_Date]+"' and session.endTime<='"+Params[end_Date]+"';";
        }
        return sqls;
    }

    @Override
    public String[] EdgesSqltransform() {
        int num=Integer.parseInt(Params[path_len])-1;
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String start="P"+i;
            String end="P"+(i+1);
            String sql="select count(*),"+start+","+end+" from streams_new,session where streams_new.session=session.session and " +
                    "session.startTime>='"+Params[start_Date]+"' and session.endTime<='"+Params[end_Date]+"' group by "+start+","+end;
            sqls[i-start_P]=sql;
        }
        return sqls;
    }
}
