package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughPredicate;

/**
 * Object representation of a "SELECT *" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class ProjectAllFieldsPredicate implements ProjectPredicate {

    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param projectOperatorId The ID of the OperatorBean to be created.
     */
    public PredicateBase generateOperatorBean(String projectOperatorId) {
        return new PassThroughPredicate(projectOperatorId);
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        return true;
    }
}
