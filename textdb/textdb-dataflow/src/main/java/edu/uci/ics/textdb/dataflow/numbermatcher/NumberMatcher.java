package edu.uci.ics.textdb.dataflow.numbermatcher;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;

/**
 * NumberMatcher is matcher for number comparison query on an integer or double field.
 *
 * @author Adrian Seungjin Lee
 *
 */
public class NumberMatcher extends AbstractSingleInputOperator {
    private NumberPredicate predicate;

    private Schema inputSchema;

    public NumberMatcher(NumberPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
    }

    @Override
    protected ITuple computeNextMatchingTuple() throws Exception {
        ITuple inputTuple = null;
        ITuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            Attribute attribute = this.predicate.getAttribute();
            DataConstants.NumberMatchingType operatorType = this.predicate.getOperatorType();

            Number threshold = this.predicate.getThreshold();
            FieldType fieldType = this.predicate.getAttribute().getFieldType();
            String fieldName = this.predicate.getAttribute().getFieldName();
            switch (fieldType) {
                case INTEGER:
                    int intValue = (int) inputTuple.getField(fieldName).getValue();
                    if (compareIntValues(intValue, threshold.intValue(), operatorType)) {
                        resultTuple = inputTuple;
                    }
                    break;
                case DOUBLE:
                    double doubleValue = (double) inputTuple.getField(fieldName).getValue();
                    if (compareDoubleValues(doubleValue, threshold.doubleValue(), operatorType)) {
                        resultTuple = inputTuple;
                    }
                    ;
                    break;
                default:
                    throw new Exception("fieldType is not set to integer or double while trying to compare numbers");
            }

            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    private boolean compareIntValues(int value, int threshold, DataConstants.NumberMatchingType operatorType) {
        switch (operatorType) {
            case EQUAL_TO:
                if (value == threshold) {
                    return true;
                }
                break;
            case GREATER_THAN:
                if (value > threshold) {
                    return true;
                }
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                if (value >= threshold) {
                    return true;
                }
                break;
            case LESS_THAN:
                if (value < threshold) {
                    return true;
                }
                break;
            case LESS_THAN_OR_EQUAL_TO:
                if (value <= threshold) {
                    return true;
                }
                break;
        }
        return false;
    }

    private boolean compareDoubleValues(double value, double threshold, DataConstants.NumberMatchingType operatorType) {
        switch (operatorType) {
            case EQUAL_TO:
                if (value == threshold) {
                    return true;
                }
                break;
            case GREATER_THAN:
                if (value > threshold) {
                    return true;
                }
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                if (value >= threshold) {
                    return true;
                }
                break;
            case LESS_THAN:
                if (value < threshold) {
                    return true;
                }
                break;
            case LESS_THAN_OR_EQUAL_TO:
                if (value <= threshold) {
                    return true;
                }
                break;
        }
        return false;
    }

    @Override
    protected void cleanUp() throws DataFlowException {
    }
}
