package edu.uci.ics.textdb.textql.statements.predicates;

/**
 * Object representation of a "SELECT *" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectAllFieldsPredicate extends SelectPredicate {

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectAllFieldsPredicate selectAllPredicate = (SelectAllFieldsPredicate) other;
        return super.equals(selectAllPredicate);
    }
}
