package resources;

import DataGenerator.SankeyDataGenerator;
import SqlTransformer.SankeySqltransformer;
import dao.LogDAO;
import entity.SankeyGraph;
import entity.SankeyGraphJsonObj;
import entity.URLNode;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2015/3/30.
 */

@Path("/session/path")
public class Sankey extends BasicResource {
    SankeyDataGenerator SanGen;

    public Sankey() {
        super("Mysql_URI","Mysql_DB");
        SanGen = new SankeyDataGenerator(uri,db);
    }

    @Path("/group")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getSankey(@QueryParam("begin") String start,@QueryParam("end") String end,
                            @QueryParam("len") String len,@QueryParam("limit") String limit) {
        String[] params={start,end,len};
        String result=null;
        try{
            SankeySqltransformer sformer=new SankeySqltransformer(params);
            SankeyGraph sankeyGraph=SanGen.getGraph(sformer.EdgesSqltransform(),sformer.NodesSqltransform());
            List<URLNode> highdrop=sankeyGraph.get_highdrop(10,20);
            List<URLNode> mainland=sankeyGraph.get_mainland(10);
            result=new SankeyGraphJsonObj(sankeyGraph.FilterByEdgeVol(Integer.parseInt(limit)),highdrop,mainland).toJson();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    @Path("/{sessionID}")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getSankeyBySessionID(@PathParam("sessionID") String sessionID,@QueryParam("len") String len) {
        String[] params={"","",len};
        String result=null;
        try{
            SankeySqltransformer sformer=new SankeySqltransformer(params);
            SankeyGraph sankeyGraph=SanGen.getGraph(sformer.EdgesSqltransformByID(sessionID),sformer.NodesSqltransformByID(sessionID));
            result=new SankeyGraphJsonObj(sankeyGraph).toJson();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
}
