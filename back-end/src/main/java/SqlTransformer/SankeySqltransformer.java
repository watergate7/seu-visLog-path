package SqlTransformer;

/**
 * Created by Administrator on 2015/3/15.
 */
public class SankeySqltransformer extends BasicSqltransformer {
    private final int start_Date=0;
    private final int end_Date=1;
    private final int path_len=2;

    public SankeySqltransformer(String[] Params){
        super(Params);
    }

    @Override
    public String[] NodesSqltransform() {
        int num=Integer.parseInt(Params[path_len]);
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String p="P"+i;
            String sql="select count(*),"+p+",streams_t.session from streams_t,session where streams_t.session=session.session and " +
                    "session.startTime>='"+Params[start_Date]+"' and session.endTime<='"+Params[end_Date]+"' and "+p+" is not NULL " +
                    "group by "+p+",streams_t.session order by "+p+";";
            sqls[i-start_P]=sql;
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
            String sql="select count(*),"+start+","+end+",streams_t.session from streams_t,session where streams_t.session=session.session and " +
                    start+" is not NULL and "+end+" is not NULL and " +
                    "session.startTime>='"+Params[start_Date]+"' and session.endTime<='"+Params[end_Date]+"' group by "+start+","+end+",streams_t.session";
            sqls[i-start_P]=sql;
        }
        return sqls;
    }

    public String[] NodesSqltransformByID(String session) {
        int num=Integer.parseInt(Params[path_len]);
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String p="P"+i;
            String sql="select count(*),"+p+",streams_t.session from streams_t where streams_t.session='"+session+"' and "+p+" is not NULL "+
                    "group by "+p+",streams_t.session order by "+p+";";
            sqls[i-start_P]=sql;
        }
        return sqls;
    }

    public String[] EdgesSqltransformByID(String session) {
        int num=Integer.parseInt(Params[path_len])-1;
        int start_P=1;
        String[] sqls=new String[num];
        for(int i=start_P;i<num+start_P;i++){
            String start="P"+i;
            String end="P"+(i+1);
            String sql="select count(*),"+start+","+end+",streams_t.session from streams_t where streams_t.session='"+session+"' and "+
                    start+" is not NULL and "+end+" is not NULL " +
                    "group by "+start+","+end+",streams_t.session";
            sqls[i-start_P]=sql;
        }
        return sqls;
    }

//    public String[] PathsSqltransformBySessionID(String session) {
//        int num=Integer.parseInt(Params[path_len])-1;
//        int start_P=1;
//        String[] sqls=new String[num];
//        for(int i=start_P;i<num+start_P;i++){
//            String start="P"+i;
//            String end="P"+(i+1);
//            String sql="select count(*),"+start+","+end+" from streams_t where streams_t.session='"+session+"' group by "+start+","+end;
//            sqls[i-start_P]=sql;
//        }
//        return sqls;
//    }
}
