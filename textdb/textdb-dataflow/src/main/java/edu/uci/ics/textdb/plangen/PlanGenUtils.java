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

}
