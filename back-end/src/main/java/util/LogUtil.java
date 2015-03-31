package util;

import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Weiwei on 2015/3/10.
 */
public class LogUtil {

    public static List<String> convertToSimpleList(List<DBObject> list) {
        List<String> stringList = new ArrayList<String>();
        for (DBObject object : list) {
            stringList.add((String)object.get("request"));
        }
        return stringList;
    }

    public static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getValue() + "\t" +pair.getKey());
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
}
