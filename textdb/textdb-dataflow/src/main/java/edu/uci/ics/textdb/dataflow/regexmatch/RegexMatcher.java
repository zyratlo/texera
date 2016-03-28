package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import org.apache.lucene.search.Query;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcher implements IOperator {
    private final String regex;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;

    public RegexMatcher(String regex, ISourceOperator sourceOperator) {
        this.regex = regex;
        this.sourceOperator = sourceOperator;
        //TODO build the luceneQuery by given regex.
    }

    public ITuple getNextTuple() {

        ITuple sourceTuple = sourceOperator.getNextTuple();
        if (match(sourceTuple)) {
            return sourceTuple;
        } else {
            return getNextTuple();
        }

    }

    private boolean match(ITuple sourceTuple) {
        return false;
    }
}
