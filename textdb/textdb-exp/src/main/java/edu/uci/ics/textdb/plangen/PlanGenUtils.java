package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.schema.AttributeType;

/**
 * This class provides a set of helper functions that are commonly used in plan generation.
 * @author Zuozhi Wang
 *
 */
public class PlanGenUtils {
    
    @FunctionalInterface
    public interface OperatorBuilder {
        public IOperator buildOperator(Map<String, String> operatorProperties) throws PlanGenException;
    }
    
    public static Map<String, OperatorBuilder> operatorBuilderMap = new HashMap<>();
    static {
    }
    
    public static IOperator buildOperator(String operatorType, Map<String, String> operatorProperties) throws PlanGenException {
        OperatorBuilder operatorBuilder = operatorBuilderMap.get(operatorType.toLowerCase());
        planGenAssert(operatorBuilder != null, 
                String.format("operatorType %s is invalid. It must be one of %s.", operatorType, operatorBuilderMap.keySet()));
               
        return operatorBuilder.buildOperator(operatorProperties);
    }
    
    
    public static void planGenAssert(boolean assertBoolean, String errorMessage) throws PlanGenException {
        if (! assertBoolean) {
            throw new PlanGenException(errorMessage);
        }
    }
    
    /**
     * This function checks if a string is a valid operator (case insensitive).
     * 
     * @param operatorStr
     * @return true if the string is an operator
     */
    public static boolean isValidOperator(String operatorStr) {
        return operatorBuilderMap.keySet().stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }

    /**
     * This function checks if a string is a valid attribute type (case insensitive).
     * 
     * @param attributeType
     * @return true if the string is an attribute type
     */
    public static boolean isValidAttributeType(String attributeType) {
        return Stream.of(AttributeType.values()).anyMatch(type -> type.toString().toLowerCase().equals(attributeType.toLowerCase()));
    }
    
    /**
     * This function converts a attributeTypeString to AttributeType (case insensitive).
     * It returns null if string is not a valid type.
     * 
     * @param attributeTypeStr
     * @return AttributeType, null if attributeTypeStr is not a valid type.
     */
    public static AttributeType convertAttributeType(String attributeTypeStr) {
        return Stream.of(AttributeType.values())
                .filter(typeStr -> typeStr.toString().toLowerCase().equals(attributeTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }
    

}
