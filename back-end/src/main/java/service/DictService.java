package service;

import com.mongodb.DB;
import dao.DictDAO;

/**
 * Created by Weiwei on 2015/3/11.
 */
public class DictService {
    DictDAO dictDAO;

    public DictService(final DB siteDatabase) {
        dictDAO = new DictDAO(siteDatabase);
    }

    public static int getUrlIndex(String url) {
        return 0;
    }
}
