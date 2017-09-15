package edu.uci.ics.texera.exp.plangen;

import edu.uci.ics.texera.api.exception.TexeraException;

/**
 * This class provides a set of helper functions that are commonly used in plan generation.
 * @author Zuozhi Wang
 *
 */
public class PlanGenUtils {

    public static void planGenAssert(boolean assertBoolean, String errorMessage) {
        if (! assertBoolean) {
            // TODO: change this to runtime plangen exception
            throw new TexeraException(errorMessage);
        }
    }

}
