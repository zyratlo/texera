package edu.uci.ics.textdb.textql.statements;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;

/**
 * Object Representation of a parsed "SELECT ... FROM ..." statement.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectStatement extends Statement {
    
    /**
     * Set to true when '*' is used as the fields to be projected, as in "SELECT * FROM ..."
     */
    public Boolean projectAll;
    
    /**
     * The { @link List } of fields to be projected if it is specified as
     * in "SELECT a, b, c FROM ..."
     */
    public List<String> projectedFields;
    
    /**
     * Predicate used for data extraction such as keyword match in "KEYWORDMATCH(a,"word")".
     */
    public ExtractPredicate extractPredicate;

    /**
     * The identifier of a view or a table name, as in "SELECT ... FROM viewName" used as 
     * source of tuples.
     */
    public String fromClause;

    /**
     * The maximum number of tuples to be returned, as in "SELECT...FROM... LIMIT 5".
     */
    public Integer limitClause;

    /**
     * The number of tuples to be skipped before returning, as in "SELECT...FROM... OFFSET 5".
     */
    public Integer offsetClause;
      
    /**
     * Create a { @code CreateViewStatement } with all the parameters set to { @code null }.
     * @param id The id of the statement.
     */
    public SelectStatement() {
        this(null, null, null, null, null, null, null);
    }

    /**
     * Create a { @code CreateViewStatement } with the given parameters.
     * @param id The ID of this statement.
     * @param projectAll If all the fields are to be projected.
     * @param projectedFields List of fields to be projected.
     * @param extractPredicate The predicate for data extraction.
     * @param fromClause The ID of the source view.
     * @param limitClause The value of the limit clause.
     * @param offsetClauseThe value of the offset clause.
     */
    public SelectStatement(String id, Boolean projectAll,
            List<String> projectedFields, ExtractPredicate extractPredicate,
            String fromClause, Integer limitClause, Integer offsetClause) {
        super(id);
        this.projectAll = projectAll;
        this.projectedFields = projectedFields;
        this.extractPredicate = extractPredicate;
        this.fromClause = fromClause;
        this.limitClause = limitClause;
        this.offsetClause = offsetClause;
    }
       
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != this.getClass()) { return false; }
        SelectStatement selectStatement = (SelectStatement) other;
        return new EqualsBuilder()
                    .appendSuper(super.equals(selectStatement))
                    .append(projectAll, selectStatement.projectAll)
                    .append(projectedFields, selectStatement.projectedFields)
                    .append(extractPredicate, selectStatement.extractPredicate)
                    .append(fromClause, selectStatement.fromClause)
                    .append(limitClause, selectStatement.limitClause)
                    .append(offsetClause, selectStatement.offsetClause)
                    .isEquals();
    }
    
}




