package resources;

import DataGenerator.SankeyDataGenerator;
import SqlTransformer.SankeySqltransformer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;

/**
 * Created by Administrator on 2015/3/30.
 */

@Path("/Sankey")
public class Sankey extends BasicResource {
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getSankey(@QueryParam("x") String x,@QueryParam("y") String y,@QueryParam("limit") String limit) {
        String[] params={x,y};
        String result="";
        try{
            SankeySqltransformer sformer=new SankeySqltransformer(params);
            result=new SankeyDataGenerator(uri,db).getGraph(sformer.EdgesSqltransform(),Integer.parseInt(limit));
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
}
