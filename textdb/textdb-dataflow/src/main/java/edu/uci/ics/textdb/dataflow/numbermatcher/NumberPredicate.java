package edu.uci.ics.textdb.dataflow.numbermatcher;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.MatchAllDocsQuery;

/**
 *
 * @author Adrian Seungjin Lee
 *
 */
public class NumberPredicate implements IPredicate {

    private Attribute attribute;
    private Number threshold;
    private DataConstants.NumberMatchingType operatorType;

    public NumberPredicate(Number threshold, Attribute attribute,
                           DataConstants.NumberMatchingType operatorType) throws DataFlowException {
        try {
            this.threshold = threshold;
            this.attribute = attribute;
            this.operatorType = operatorType;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    public DataConstants.NumberMatchingType getOperatorType() {
        return operatorType;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Number getThreshold() {
        return threshold;
    }

}
