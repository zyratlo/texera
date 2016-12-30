package edu.uci.ics.textdb.textql.statements.predicates;

/**
 * Object representation of a "SELECT (...)" predicate inside a { @code SelectExtractStatement }.
 * Subclasses have specific fields related to its projection functionalities.
 * SelectPredicate --+ SelectAllPredicate
 *                   + SelectFieldsPredicate
 * 
 * @author Flavio Bayer
 *
 */
public abstract class SelectPredicate {

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectPredicate selectPredicate = (SelectPredicate) other;
        return true;
    }
}
