package dao;

import com.mongodb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogDAO {
    private DBCollection log;

    public LogDAO(final DB siteDatabase) {
        log = siteDatabase.getCollection("log");
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
}
