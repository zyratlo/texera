package edu.uci.ics.textdb.web.request;

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

    public void aggregateOperatorProperties() {
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
            }
        }
    }
}
