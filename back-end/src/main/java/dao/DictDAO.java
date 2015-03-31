package dao;

import com.mongodb.*;

/**
 * Created by Weiwei on 2015/3/11.
 */
public class DictDAO {
    private DBCollection dict;

    public DictDAO(final DB siteDatabase) { dict = siteDatabase.getCollection("dict"); }

    public Integer getUrlIndex(String url) {
        QueryBuilder builder = QueryBuilder.start("url").is(url);
        DBObject object = dict.findOne(builder.get());
        if (object == null)
            return -1;
        else
            return (Integer)object.get("id");
    }
}
