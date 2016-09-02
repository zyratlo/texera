package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataTypeConstants;
import edu.uci.ics.textdb.common.constants.OperatorConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;

/**
 * This class provides a set of helper functions that are commonly used in plan generation.
 * @author Zuozhi Wang
 *
 */
public class PlanGenUtils {
    
    @FunctionalInterface
    public interface OperatorBuilderFunc {
        public IOperator build(Map<String, String> operatorProperties) throws Exception;
    }
    
    /**
     * A map of operators to the their builder classes.
     */
    public static final Map<String, OperatorBuilderFunc> operatorBuilderMap;
    static {
        operatorBuilderMap = new HashMap<>();
        operatorBuilderMap.put("KeywordMatcher".toLowerCase(), KeywordMatcherBuilder::buildOperator);
    }
    
    
    public static void planGenAssert(boolean assertBoolean) throws PlanGenException {
        planGenAssert(assertBoolean, "");
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
        return OperatorConstants.operatorList.stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }

    /**
     * This function checks if a string is a valid attribute type (case insensitive).
     * 
     * @param attributeType
     * @return true if the string is an attribute type
     */
    public static boolean isValidAttributeType(String attributeType) {
        return DataTypeConstants.attributeTypeList.stream().anyMatch(str -> str.toLowerCase().equals(attributeType.toLowerCase()));
    }

    /**
     * This function builds the operator based on the type and properties.
     * 
     * @param operatorType
     * @param operatorProperties
     * @return operator that is built
     * @throws Exception
     */
    public static IOperator buildOperator(String operatorType, Map<String, String> operatorProperties) throws Exception {
        OperatorBuilderFunc operatorBuilderFunc = operatorBuilderMap.get(operatorType.toLowerCase());       
        return operatorBuilderFunc.build(operatorProperties);
    }

}
