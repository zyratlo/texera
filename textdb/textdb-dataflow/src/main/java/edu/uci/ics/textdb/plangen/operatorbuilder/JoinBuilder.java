package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.join.Join;
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
    
    public static final String JOIN_PREDICATE = "predicateType";
    
    public static final String JOIN_ID_ATTRIBUTE_NAME = "idAttributeName";
    public static final String JOIN_ID_ATTRIBUTE_TYPE = "idAttributeType";
    
    public static final String JOIN_CHARACTER_DISTANCE = "characterDistance";
    
        
    public static IOperator buildOperator(Map<String, String> operatorProperties) throws PlanGenException {        
        String joinPredicateType = OperatorBuilderUtils.getRequiredProperty(JOIN_PREDICATE, operatorProperties);
        PlanGenUtils.planGenAssert(! joinPredicateType.trim().isEmpty(), "Join predicate type is empty");
        
        IJoinPredicate joinPredicate = generateJoinPredicate(joinPredicateType, operatorProperties);
        Join joinOperator = new Join(null, null, joinPredicate);
        
        return joinOperator;
    }
    
    
    @FunctionalInterface
    private interface GetJoinPredicate {
        IJoinPredicate getJoinPredicate(Map<String, String> operatorProperties) throws PlanGenException;
    }
    
    
    private static JoinDistancePredicate getJoinCharDistancePredicate(
            Map<String, String> operatorProperties) throws PlanGenException{
        String distanceStr = OperatorBuilderUtils.getRequiredProperty(JOIN_ID_ATTRIBUTE_TYPE, operatorProperties);
        String joinIDAttributeName = OperatorBuilderUtils.getRequiredProperty(JOIN_ID_ATTRIBUTE_NAME, operatorProperties);
        String joinIDAttributeType = OperatorBuilderUtils.getRequiredProperty(JOIN_ID_ATTRIBUTE_TYPE, operatorProperties);
        
        PlanGenUtils.planGenAssert(! joinIDAttributeName.trim().isEmpty(), 
                "Join character distance predicate: ID attribute name is empty.");
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidAttributeType(joinIDAttributeType), 
                "Join character distance predicate: ID attribute type is invalid.");
        Attribute joinIDAttribute = new Attribute(joinIDAttributeName, PlanGenUtils.convertAttributeType(joinIDAttributeType));
        
        
        List<Attribute> attributeList = OperatorBuilderUtils.constructAttributeList(operatorProperties);       
        PlanGenUtils.planGenAssert(attributeList.size() == 1, 
                "Join character distance predicate allows only 1 attribute, got " + attributeList.size()+  " attributes.");
        Attribute joinAttribute = attributeList.get(0);
        
        int distance;
        try {
            distance = Integer.parseInt(distanceStr);
        } catch (NumberFormatException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        if (distance <= 0) {
            throw new PlanGenException("Join character distance predicate: distance must be greater than 0.");
        }
        
        return new JoinDistancePredicate(joinIDAttribute, joinAttribute, distance);
    }
    
    private static HashMap<String, GetJoinPredicate> joinPredicateHandlerMap = new HashMap<>();
    static {
        joinPredicateHandlerMap.put("CharacterDistance".toLowerCase(), JoinBuilder::getJoinCharDistancePredicate);
    }
    
    private static IJoinPredicate generateJoinPredicate(String joinPredicateType, Map<String, String> operatorProperties) throws PlanGenException {
        PlanGenUtils.planGenAssert(joinPredicateHandlerMap.containsKey(joinPredicateType.toLowerCase()), 
                "Join predicate type is invalid, it must be one of " + joinPredicateHandlerMap.keySet());
        return joinPredicateHandlerMap.get(joinPredicateType).getJoinPredicate(operatorProperties);
    }

}
