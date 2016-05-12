package edu.uci.ics.textdb.api.plan;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;

/**
 * Created by chenli on 5/11/16.
 * <p>
 * A plan is a tree of operators.
 */
public class Plan {

    private final ISink root;

    public Plan(ISink root) {
        this.root = root;
    }

    public ISink getRoot() {
        return root;
    }
}
