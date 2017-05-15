package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.plangen.LogicalPlan;
import edu.uci.ics.textdb.exp.planstore.PlanStore;
import edu.uci.ics.textdb.exp.planstore.PlanStoreConstants;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.web.TextdbWebException;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;
import edu.uci.ics.textdb.web.response.planstore.QueryPlanBean;
import edu.uci.ics.textdb.web.response.planstore.QueryPlanListBean;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kishorenarendran on 2/24/17.
 */
@Path("/planstore")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PlanStoreResource {

    private static PlanStore planStore;
    private static ObjectMapper mapper;

    static {
        try {
            planStore = PlanStore.getInstance();
            mapper = new ObjectMapper();
            mapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            planStore.createPlanStore();
        }
        catch (TextDBException e) {
            e.printStackTrace();
        }
    }

    @GET
    public QueryPlanListBean getAllQueryPlans() {
        ArrayList<QueryPlanBean> queryPlans = new ArrayList<>();

        try {
            // Getting an iterator for the plan store
            DataReader reader = planStore.getPlanIterator();
            reader.open();

            // Iterating through the stored plans, and mapping them to a QueryPlanRequest object
            Tuple tuple;
            while ((tuple = reader.getNextTuple()) != null) {
                String name = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
                String description = tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString();
                String logicalPlanJson = tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString();
                queryPlans.add(new QueryPlanBean(name, description,
                        mapper.readValue(logicalPlanJson, LogicalPlan.class)));
            }
        }
        catch(TextDBException e) {
            e.printStackTrace();
            throw new TextdbWebException(e.getMessage());
        }
        catch(IOException e) {
            e.printStackTrace();
            throw new TextdbWebException("fail to parse json:\n" + e.getMessage());
        }
        return new QueryPlanListBean(queryPlans);
    }

    @GET
    @Path("/{plan_name}")
    public QueryPlanBean getQueryPlan(@PathParam("plan_name") String planName) {
        try {
            Tuple tuple = planStore.getPlan(planName);
            if(tuple == null) {
                throw new TextdbWebException("Plan with the given name does not exist");
            }
            QueryPlanBean queryPlanBean = new QueryPlanBean(tuple.getField(PlanStoreConstants.NAME).getValue().toString(),
                    tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString(),
                    mapper.readValue(tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString(), LogicalPlan.class));
            return queryPlanBean;
        }
        catch(TextDBException e) {
            throw new TextdbWebException(e.getMessage());
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @POST
    public TextdbWebResponse addQueryPlan(String queryPlanBeanJson) {
        try {
            QueryPlanBean queryPlanBean = new ObjectMapper().readValue(queryPlanBeanJson, QueryPlanBean.class);
            // Adding the query plan to the PlanStore
            planStore.addPlan(queryPlanBean.getName(), queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch(TextDBException e) {
            throw new TextdbWebException(e.getMessage());
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return new TextdbWebResponse(0, "Success");
    }

    @DELETE
    @Path("/{plan_name}")
    public TextdbWebResponse deleteQueryPlan(@PathParam("plan_name") String planName) {
        try {
            // Deleting the plan from the plan store
            planStore.deletePlan(planName);
        }
        catch(TextDBException e) {
            throw new TextdbWebException(e.getMessage());
        }
        return new TextdbWebResponse(0, "Success");
    }

    @PUT
    @Path("/{plan_name}")
    public TextdbWebResponse updateQueryPlan(@PathParam("plan_name") String planName, String queryPlanBeanJson) {
        try {
            QueryPlanBean queryPlanBean = new ObjectMapper().readValue(queryPlanBeanJson, QueryPlanBean.class);
            // Updating the plan in the plan store
            planStore.updatePlan(planName, queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch(IOException | TextDBException e) {
            throw new TextdbWebException(e.getMessage());
        }
        return new TextdbWebResponse(0, "Success");
    }
}