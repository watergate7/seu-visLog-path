package dao;

import com.mongodb.*;
import entity.URLNode;
import com.google.gson.Gson;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogDAO {
    private DBCollection log;

    public LogDAO(final DB siteDatabase) {
        log = siteDatabase.getCollection("log");
    }

    public LogDAO(final String mongoURI,final String db) throws UnknownHostException{
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        DB siteDatabase = mongoClient.getDB(db);
        log=siteDatabase.getCollection("log");
    }

    /**
     * Get a session list, no duplicate session
     * @return session list, {session, nums}
     */
    public List<DBObject> getSeperateSessions() {

        // $group
        DBObject groupFields = new BasicDBObject("_id", "$session");
        groupFields.put("nums", new BasicDBObject("$sum", 1));
        DBObject group = new BasicDBObject("$group", groupFields);

        // $sort
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("nums", -1));

        List<DBObject> pipeline = Arrays.asList(group, sort);
        AggregationOutput output = log.aggregate(pipeline);

        // output
        List<DBObject> sessionList = new ArrayList<DBObject>();
        for (DBObject result : output.results()) {
            sessionList.add(result);
        }

        return sessionList;
    }

    public List<DBObject> getSessionSequenceById(String sessionId) {
        QueryBuilder builder = QueryBuilder.start("session").is(sessionId);

        DBCursor cursor = log.find(builder.get(), new BasicDBObject("_id", false))
                .sort(new BasicDBObject("session_seq", 1));

        // output
        List<DBObject> singleSessionSequence = new ArrayList<DBObject>();
        while(cursor.hasNext()) {
            DBObject item = cursor.next();
            singleSessionSequence.add(item);
            //System.out.println(item.get("session_seq" + " | " ));
        }

        cursor.close();

        return singleSessionSequence;
    }

    public String getPathBySession(String session){
        Gson gson=new Gson();
        List<DBObject> dbobjs=this.getSessionSequenceById(session);
        ArrayList<URLNode> nodes=new ArrayList<URLNode>();
        for(int i=0;i<dbobjs.size();i++){
            DBObject ob=dbobjs.get(i);
            URLNode node=new URLNode();
            if(i==0){
                URLNode first=new URLNode();
                first.setUrl(ob.get("referer").toString());
                first.setDatetime("-");
                nodes.add(first);
            }
            node.setUrl(ob.get("request").toString());
            node.setDatetime(ob.get("dateTime").toString());
            nodes.add(node);
        }
        return  gson.toJson(nodes);
    }
}
