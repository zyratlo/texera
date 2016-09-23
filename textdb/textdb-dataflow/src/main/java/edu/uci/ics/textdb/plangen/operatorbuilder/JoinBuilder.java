package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.PlanGenUtils;


/**
 * JoinBuilder provides a static function that builds a Join operator.
 * 
 * JoinBuilder currently needs the following properties:
 * 
 *   predicate (required)
 *   
 *   properties required for constructing attributeList, see OperatorBuilderUtils.constructAttributeList
 * 
 * @author Zuozhi Wang
 *
 */
public class JoinBuilder {
    
    public static final String JOIN_PREDICATE = "predicate";
    
    public static IOperator buildOperator(Map<String, String> operatorProperties) throws PlanGenException {
        String joinPredicateString = OperatorBuilderUtils.getRequiredProperty(JOIN_PREDICATE, operatorProperties);
        
        // generate attribute list
        List<Attribute> attributeList = OperatorBuilderUtils.constructAttributeList(operatorProperties);
        
        PlanGenUtils.planGenAssert(attributeList.size() == 1, 
                "Join operator allows only 1 attribute, got " + attributeList.size()+  " attributes.");

        JSONObject joinPredicateJsonObject = new JSONObject(joinPredicateString);
        String joinPredicateType = joinPredicateJsonObject.getString(key);
        
        
        
        return null;
    }

}
