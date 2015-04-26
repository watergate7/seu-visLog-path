package me.wwsun;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import junit.framework.TestCase;
import dao.LogDAO;
import service.LogService;

import java.net.UnknownHostException;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    final String mongoURI = "mongodb://223.3.75.101:27017";
//    "mongodb://223.3.80.243:27017";
    final MongoClient mongoClient;
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName ) throws UnknownHostException {
        super( testName );
        mongoClient = new MongoClient(new MongoClientURI(mongoURI));
    }

    public void testSessionSize() {
        final DB siteDatabase = mongoClient.getDB("jiaodian");
        LogDAO logDAO = new LogDAO(siteDatabase);
        int length = logDAO.getSeperateSessions().size();
        List<DBObject> obs=logDAO.getSeperateSessions();
        for(DBObject ob:obs){
            System.out.println(ob.toString());
        }
        System.out.println(length);
        assertTrue(length>0);
    }

    public void testSessionSequence() {
        final DB siteDatabase = mongoClient.getDB("jiaodian");
        LogDAO logDAO = new LogDAO(siteDatabase);

        String session = "zcuMjE2LjMuNjMyMDE0MDgxMDAyMzU0ODU1MjI5MDU4ODAM";
        int lenght = logDAO.getSessionSequenceById(session).size();
        assertTrue(lenght>0);
    }

    public void testAllSequences() {
        final DB siteDatabase = mongoClient.getDB("jiaodian");
        LogService logService = new LogService(siteDatabase);
        logService.getAllSequences();
    }
}
