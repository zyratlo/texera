package edu.uci.ics.textdb.web.request;

import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.web.request.beans.DictionaryMatcherBean;
import edu.uci.ics.textdb.web.request.beans.LinkBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This class describes the JSON edu.uci.ics.textdb.web.request when a logical query plan is submitted.
 * It also contains the HashMap that is a representation of all the operators'
 * properties for a given logical query plan.
 * Created by kishorenarendran on 10/21/16.
 */
public class QueryPlanRequest {
    private ArrayList<OperatorBean> operators;
    private ArrayList<LinkBean> links;
    private HashMap<String, HashMap<String, String>> operatorProperties;
    private LogicalPlan logicalPlan;

    public static final String GET_PROPERTIES_FUNCTION_NAME = "getOperatorProperties";
    public static final HashMap<String, Class> OPERATOR_BEAN_MAP = new HashMap<String, Class>() {{
        put("DictionaryMatcher", DictionaryMatcherBean.class);
    }};

    public QueryPlanRequest() {
    }

    public QueryPlanRequest(ArrayList<OperatorBean> operators, ArrayList<LinkBean> links) {
        this.operators = operators;
        this.links = links;
    }

    public ArrayList<OperatorBean> getOperators() {
        return operators;
    }

    public void setOperators(ArrayList<OperatorBean> operators) {
        this.operators = operators;
    }

    public ArrayList<LinkBean> getLinks() {
        return links;
    }

    public void setLinks(ArrayList<LinkBean> links) {
        this.links = links;
    }

    public HashMap<String, HashMap<String, String>> getOperatorProperties() {
        return operatorProperties;
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    /**
     * This method parses and aggregates the properties of all the operators in the operators data members. It returns
     * a false flag when there are any exceptions thrown when trying to create the operator properties HashMap
     * @return - A boolean to denote the status of aggregating the different operators' properties
     */
    public boolean aggregateOperatorProperties() {
        this.operatorProperties = new HashMap<>();
        for(Iterator<OperatorBean> iter = operators.iterator(); iter.hasNext(); ) {
            OperatorBean operatorBean = iter.next();
            Class operatorBeanClassName =  OPERATOR_BEAN_MAP.get(operatorBean.getOperatorType());
            try {
                Method method = operatorBeanClassName.getMethod(GET_PROPERTIES_FUNCTION_NAME);
                HashMap<String, String> currentOperatorProperty = (HashMap<String, String>) method.invoke(operatorBean);
                this.operatorProperties.put(operatorBean.getOperatorID(), currentOperatorProperty);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // If any exception arises a NULL HashMap is raised
                this.operatorProperties = null;
                return false;
            }
        }
        return true;
    }

    /**
     * This method uses the OperatorBeans, OperatorProperties and the LinkBeans, to create an instance of the Logical
     * Plan class, any exceptions and failures thrown when adding an operator or a link is considered a failure and a
     * false value is returned, if not a true values is returned
     * @return - A boolean to denote the status of LogicalPlan creation
     */
    public boolean createLogicalPlan() {
        logicalPlan = new LogicalPlan();
        for(Iterator<OperatorBean> iterator = operators.iterator(); iterator.hasNext(); ) {
            try {
                OperatorBean operatorBean = iterator.next();
                logicalPlan.addOperator(operatorBean.getOperatorID(), operatorBean.getOperatorType(), operatorProperties.get(operatorBean.getOperatorID()));
            }
            catch(PlanGenException e) {
                // If a PlanGenException occurs the logical plan object is assigned to NULL, and a false value is returned
                this.logicalPlan = null;
                return false;
            }
        }

        for(Iterator<LinkBean> iterator = links.iterator(); iterator.hasNext(); ) {
            try {
                LinkBean linkBean = iterator.next();
                logicalPlan.addLink(linkBean.getFromOperatorID(), linkBean.getToOperatorID());
            }
            catch(PlanGenException e) {
                // If a PlanGenException occurs the logical plan object is assigned to NULL, and a false value is returned
                this.logicalPlan = null;
                return false;

            }
        }

        // Returning success on complete successful creation of a logical plan
        return true;
    }
}
