package resources;

import dao.LogDAO;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2015/4/22.
 */
@Path("/session/sequence")
public class Sequence extends BasicResource{
    LogDAO logDAO;

    public Sequence()throws UnknownHostException{
        super("Mongo_URI","Mongo_DB");
        logDAO=new LogDAO(uri,db);
    }

    @Path("/{sessionID}")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getSequenceBySessionID(@PathParam("sessionID") String sessionID) {
        String path=logDAO.getPathBySession(sessionID);
        return path;
    }
}
