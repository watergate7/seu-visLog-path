package service;

import com.mongodb.DB;
import com.mongodb.DBObject;
import dao.LogDAO;
import util.LogUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Weiwei on 2015/3/10.
 */
public class LogService {
    LogDAO logDAO;

    public LogService(final DB siteDatabase) { logDAO = new LogDAO(siteDatabase); }

    public void getAllSequences() {
        List<DBObject> sessionList = logDAO.getSeperateSessions();
        Map<String, Integer> urlMap = new HashMap<String, Integer>();

        int counterId = 0;

        for (DBObject session : sessionList) {
            String sessionId = (String) session.get("_id");
            List<DBObject> singleSessionList = logDAO.getSessionSequenceById(sessionId);
            List<String> singleSimpleList = LogUtil.convertToSimpleList(singleSessionList);

            for (String str : singleSimpleList) {

                if(!urlMap.containsKey(str)) {
                    urlMap.put(str, counterId++);
                }

                System.out.print(urlMap.get(str) + "\t->\t");
            }
            System.out.println("");
        }

        System.out.println("=======================");
        LogUtil.printMap(urlMap);
    }
}
