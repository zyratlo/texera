package edu.uci.ics.textdb.web.resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.plangen.LogicalPlan;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.web.TextdbWebException;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.textdb.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 * 
 * @author Kishore
 * @author Zuozhi
 */
@Path("/newqueryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NewQueryPlanResource {
    
    public static java.nio.file.Path resultDirectory = Paths.get(Utils.getTextdbHomePath(), "result");
    
    /**
     * This is the edu.uci.ics.textdb.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TextdbWebResponse object
     */
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(String logicalPlanJson) {
        try {
            LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);
            Plan plan = logicalPlan.buildQueryPlan();
            ISink sink = plan.getRoot();
            
            // send response back to frontend
            if (sink instanceof TupleSink) {
                TupleSink tupleSink = (TupleSink) sink;
                tupleSink.open();
                List<Tuple> results = tupleSink.collectAllTuples();
                tupleSink.close();
                
                // generate new UUID as the result id
                String resultID = UUID.randomUUID().toString();
                
                // write original json of the result into a file                
                java.nio.file.Path resultFile = resultDirectory.resolve(resultID + ".json");
                if (Files.notExists(resultDirectory)) {
                    Files.createDirectories(resultDirectory);
                }
                Files.createFile(resultFile);
                Files.write(resultFile, new ObjectMapper().writeValueAsBytes(results));
                
                // put readable json of the result into response
                ArrayNode resultNode = new ObjectMapper().createArrayNode();
                for (Tuple tuple : results) {
                    resultNode.add(tuple.getReadableJson());
                }
                
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", 0);
                response.set("result",resultNode);
                response.put("resultID", resultID);
                return response;
            } else {
                // execute the plan and return success message
                Engine.getEngine().evaluate(plan);
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", 1);
                response.put("message", "plan sucessfully executed");
                return response;
            }
            
        } catch ( IOException | RuntimeException e) {
            // TODO remove RuntimeException after the exception refactor
            e.printStackTrace();
            throw new TextdbWebException(e.getMessage());
        }   
    }

}
