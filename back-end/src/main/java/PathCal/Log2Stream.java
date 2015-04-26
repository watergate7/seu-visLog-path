package PathCal;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import dao.DictDAO;
import dao.LogDAO;
import dao.StreamDAO;

import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by Administrator on 2015/3/14.
 */
public class Log2Stream {
    public final static int url=0;
    public final static int go=1;
    public final static int url_level=2;
    public final static int go_level=3;
    public final static int dateTime=4;
    public final static int go_dateTime=5;
    final int struc_size=6;


//    public Log2Stream(String params[]) throws UnknownHostException {
//        mongoURI=params[0];
//        db=params[1];
//        mongoClient = new MongoClient(new MongoClientURI(mongoURI));
//    }
//
//    public List<DBObject> getSequenceBySession(String session) {
//        final DB siteDatabase = mongoClient.getDB(db);
//        LogDAO logDAO = new LogDAO(siteDatabase);
//        return logDAO.getSessionSequenceById(session);
//    }
//
//    public Integer getIndxByUrl(String url) {
//        final DB siteDatabase = mongoClient.getDB(db);
//        DictDAO dictDAO = new DictDAO(siteDatabase);
//        return dictDAO.getUrlIndex(url);
//    }

    public List<ArrayList<String[]>> getStreamsOfSequence(List<DBObject> sequence){
        List<ArrayList<String[]>> streams=new ArrayList<ArrayList<String[]>>();

        for(DBObject ob:sequence) {
            if(ob.get("referer").toString().equals(ob.get("request").toString())){
                continue;
            }
            else{
                String[] point=new String[struc_size];
                point[url]=ob.get("referer").toString();
                point[go]=ob.get("request").toString();
                point[dateTime]="nul";
                point[go_dateTime]=ob.get("dateTime").toString();
                //level3数据库中改成了level3_referer和request,这里暂时用不到不需要改
                if(ob.get("level3")!=null)
                    point[go_level]=ob.get("level3").toString();
                else
                    point[go_level]="nul";
                if(streams.size()==0){
                    List<String[]> stream=new ArrayList<String[]>();
                    point[url_level]="nul";
                    stream.add(point);
                    streams.add((ArrayList)stream);
                }
                else{
                    int k;
                    for(k=streams.size()-1;k>=0;k--){
                        List<String[]> stream=streams.get(k);
                        if(stream.get(stream.size()-1)[go].equals(ob.get("referer"))){
                            point[url_level]=stream.get(stream.size() - 1)[go_level];
                            stream.add(point);
                            break;
                        }
                        else{
                            int index=-1;
                            for(int j=stream.size()-2;j>=0;j--){
                                if(stream.get(j)[go].equals(ob.get("referer"))){
                                    index=j;
                                    break;
                                }
                            }
                            if(index!=-1){
                                List<String[]> newstream=new ArrayList<String[]>();
                                for(int j=0;j<=index;j++){
                                    newstream.add(stream.get(j));
                                }
                                point[url_level]=stream.get(index)[go_level];
                                newstream.add(point);
                                streams.add((ArrayList)newstream);
                                break;
                            }
                        }
                    }
                    if(k==-1){
                        List<String[]> newstream=new ArrayList<String[]>();
                        point[url_level]="nul";
                        newstream.add(point);
                        streams.add((ArrayList)newstream);
                    }
                }
            }
        }

        //对streams做一次遍历，从stream中剔除起点是-1(即referer字段为“-”)的点、在stream末尾添加go_url
        Iterator<ArrayList<String[]>> iter=streams.iterator();
        while(iter.hasNext()){
            List<String[]> stream=iter.next();
            //设置每个节点对应的datetime
            for(int i=1;i<stream.size();i++){
                stream.get(i)[dateTime]=stream.get(i-1)[go_dateTime];
            }
            //在stream末尾添加go_url
            String[] lastpoint=stream.get(stream.size()-1);
            String[] point=new String[struc_size];
            point[url]=lastpoint[go];
            point[url_level]=lastpoint[go_level];
            point[dateTime]=lastpoint[go_dateTime];
            point[go_dateTime]="nul";
            point[go]="nul";
            point[go_level]="nul";
            stream.add(point);

            String[] first=stream.get(0);
            if(!Pattern.matches(".*made-in-china.*",first[url])){
                stream.remove(0);
            }

//            if(stream.size()==1&&stream.get(0)[url].equals("-")){
//                iter.remove();
//            }
//            else{
////                if(stream.get(0)[url].equals("-"))
////                    stream.remove(0);
//                String[] lastpoint=stream.get(stream.size()-1);
//                String[] point=new String[struc_size];
//                point[url]=lastpoint[go];
//                point[url_level]=lastpoint[go_level];
//                point[go]="NULL";
//                point[go_level]="NULL";
//                stream.add(point);
//            }
        }

        return streams;
    }

    public static void main(String args[]) throws UnknownHostException {
        long t1=System.currentTimeMillis();
//        String mongoURI="mongodb://223.3.75.101:27017";
        String mongoURI="mongodb://223.3.80.243:27017";
        String db="jiaodian";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        DB siteDatabase = mongoClient.getDB(db);
        LogDAO logDAO = new LogDAO(siteDatabase);
        DictDAO dictDAO = new DictDAO(siteDatabase);
//        String params[]={
//                "mongodb://223.3.75.101:27017",
//                "jiaodian"
//        };
        Log2Stream log = new Log2Stream();

        List<DBObject> sessions=logDAO.getSeperateSessions();

        //每100个session写一次数据库
        int count=100;
        //存放100个session对应的路径
        Map<String,List<ArrayList<String[]>>> BatchStreams=new HashMap<String, List<ArrayList<String[]>>>();
        StreamDAO strDao = new StreamDAO("jdbc:mysql://localhost:3306","log_stream",dictDAO);
        System.out.println("session:"+sessions.size());
        for(int index=0;index<sessions.size();index++){
            DBObject session_obj=sessions.get(index);
//            System.out.println(index);
                String session=session_obj.get("_id").toString();
//                if(session.equals("kV1TkRVdU1UUXlMak14TWpBeE5EQTRNRGt5TVRFd016VXlOalE0TWpNeU9Ea3dNQU0e")){
//                    System.out.println("Session:"+session);
                    List<DBObject> sequence = logDAO.getSessionSequenceById(session);
                    List<ArrayList<String[]>> streams=log.getStreamsOfSequence(sequence);

//                    for(List<String[]> stream:streams){
//                        for(String[] point:stream) {
////                            System.out.print(point[log.url]+"->");
//                            System.out.print(dictDAO.getUrlIndex(point[log.url])+"->");
////                            if(dictDAO.getUrlIndex(point[log.url])==-1)
////                                System.out.println(point[log.url]);
//                        }
//                        System.out.println();
//                    }

                    String[] empty={"nul","nul","nul","nul","nul","nul"};
                    for(List<String[]> stream:streams){
                        int size=stream.size();
                        //保存路径的最后一个节点，便于计算
                        String[] endP=null;
                        if(size<10){
                            endP=stream.get(size-1);
                            for(int i=size;i<10;i++)
                                stream.add(empty);
                        }
                        else if(size>=10){
                            endP=stream.get(9);
                            for(int i=size-1;i>=10;i--)
                                stream.remove(i);
                        }
                        stream.add(endP);
                    }

                    BatchStreams.put(session,streams);
                    if(--count==0||index==sessions.size()-1){
                        strDao.insertBatchStreams(BatchStreams);
                        BatchStreams=new HashMap<String, List<ArrayList<String[]>>>();
                        count=100;
                    }
//                    StreamDAO strDao = new StreamDAO("jdbc:mysql://localhost:3306","log_stream",dictDAO);
//                    strDao.insertStreams(streams,session);
//                    strDao.disCon();
//            }
        }
        strDao.disCon();

//        String session = "zcuMjE2LjMuNjMyMDE0MDgxMDAyMzU0ODU1MjI5MDU4ODAM";

//        System.out.println("======================");
//        for(List<String[]> stream:streams){
//            for(String[] point:stream) {
//                System.out.print(log.getIndxByUrl(point[log.url])+"->");
//            }
//            System.out.println();
//        }
        long t2=System.currentTimeMillis();
        System.out.println((double)(t2-t1)/1000/3600);
    }
}
