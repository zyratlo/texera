package edu.uci.ics.textdb.textql.statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.OperatorLinkBean;

/**
 * Object Representation of a parsed "SELECT ... FROM ..." statement.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectExtractStatement extends Statement {
    
    /**
     * Predicate used for projection of the fields to be returned such as in "SELECT *".
     */
    private SelectPredicate selectPredicate;
    
    /**
     * Predicate used for data extraction such as keyword match in "KEYWORDMATCH(a,"word")".
     */
    private ExtractPredicate extractPredicate;

    /**
     * The identifier of a view or a table name, as in "SELECT ... FROM viewName" used as 
     * source of tuples.
     */
    private String fromClause;

    /**
     * The maximum number of tuples to be returned, as in "SELECT...FROM... LIMIT 5".
     */
    private Integer limitClause;

    /**
     * The number of tuples to be skipped before returning, as in "SELECT...FROM... OFFSET 5".
     */
    private Integer offsetClause;
      
    /**
     * Create a { @code CreateViewStatement } with all the parameters set to { @code null }.
     * @param id The id of the statement.
     */
    public SelectExtractStatement() {
        this(null, null, null, null, null, null);
    }

    /**
     * Create a { @code CreateViewStatement } with the given parameters.
     * @param id The ID of this statement.
     * @param selectPredicate The predicate for result projection.
     * @param extractPredicate The predicate for data extraction.
     * @param fromClause The ID of the source view.
     * @param limitClause The value of the limit clause.
     * @param offsetClauseThe value of the offset clause.
     */
    public SelectExtractStatement(String id, SelectPredicate selectPredicate, ExtractPredicate extractPredicate,
            String fromClause, Integer limitClause, Integer offsetClause) {
        super(id);
        this.selectPredicate = selectPredicate;
        this.extractPredicate = extractPredicate;
        this.fromClause = fromClause;
        this.limitClause = limitClause;
        this.offsetClause = offsetClause;
    }
    
    /**
     * Get the select predicate.
     * @return The select predicate.
     */
    public SelectPredicate getSelectPredicate() {
        return selectPredicate;
    }
    
    /**
     * Set the select predicate.
     * @param selectPredicate The select predicate to be set.
     */
    public void setSelectPredicate(SelectPredicate selectPredicate) {
        this.selectPredicate = selectPredicate;
    }
    
    /**
     * Get the extract predicate.
     * @return The extract predicate.
     */
    public ExtractPredicate getExtractPredicate() {
        return extractPredicate;
    }

    /**
     * Set the extract predicate.
     * @param extractPredicate The extract predicate to be set.
     */
    public void setExtractPredicate(ExtractPredicate extractPredicate) {
        this.extractPredicate = extractPredicate;
    }

    /**
     * Get the value of the from clause.
     * @return The value of the from clause.
     */
    public String getFromClause() {
        return fromClause;
    }

    /**
     * Set the value of the from clause.
     * @param fromClause The new value for the from clause.
     */
    public void setFromClause(String fromClause) {
        this.fromClause = fromClause;
    }

    /**
     * Get the value of the limit clause.
     * @return The value of the limit clause.
     */
    public Integer getLimitClause() {
        return limitClause;
    }

    /**
     * Set the value of the limit clause.
     * @param limitClause The new value for the limit clause.
     */
    public void setLimitClause(Integer limitClause) {
        this.limitClause = limitClause;
    }

    /**
     * Get the value of the offset clause.
     * @return The value of the offset clause.
     */
    public Integer getOffsetClause() {
        return offsetClause;
    }

    /**
     * Set the value of the offset clause.
     * @param offsetClause The new value for the offset clause.
     */
    public void setOffsetClause(Integer offsetClause) {
        this.offsetClause = offsetClause;
    }
        
    @Override
    public String getInputNodeID(){
        // Append "_s" to the id of the statement to create the id of the bean, where "s" stands for Source.
        return super.getId() + "_s";
    }    
    
    @Override
    public String getOutputNodeID(){
        return super.getId();
    }

    /**
     * Get the name of the bean built by the select predicate.
     * @return The name of the bean built by the select predicate.
     */
    private String getSelectionNodeID(){
        // Append "_p" to the id of the statement to create the id of the bean, where "p" stands for Projection.
        return super.getId() + "_p";
    }
    
    /**
     * Get the name of the bean built by the extract predicate.
     * @return The name of the bean built by the extract predicate.
     */
    private String getExtractionNodeID(){
        // Append "_e"  to the id of the statement to create the id of the bean, where "e" stands for Extraction.
        return super.getId() + "_e";
    }
    
    /**
     * Return a list of operators generated when this statement is converted to beans.
     * Beans will be generated for the Alias, Projection, Extraction and Source operators.
     * @return The list of operator beans generated by this statement.
     */
    @Override
    public List<OperatorBean> getInternalOperatorBeans(){
        List<OperatorBean> operators = new ArrayList<>();
        // Build and append a PassThroughBean as an alias for this Statement
        operators.add(new PassThroughBean(getOutputNodeID(), "PassThrough"));
        // Build and append bean for Projection
        if(this.selectPredicate==null){
            operators.add(new PassThroughBean(getSelectionNodeID(), "PassThrough"));
        }else{
            operators.add(this.selectPredicate.generateOperatorBean(getSelectionNodeID()));
        }
        // Build and append bean for Extraction predicate
        if(this.extractPredicate==null){
            operators.add(new PassThroughBean(getExtractionNodeID(), "PassThrough"));
        }else{
            operators.add(this.extractPredicate.generateOperatorBean(getExtractionNodeID()));
        }
        // Build and append bean for the Source
        operators.add(new PassThroughBean(getInputNodeID(), "PassThrough"));
        // return the built operators
        return operators;
    }
    
    /**
     * Return a list of links generated when this statement is converted to beans.
     * Beans will be generated for the links between Alias, Projection, Extraction and Source predicates.
     * @return The list of link beans generated by this statement.
     */
    @Override
    public List<OperatorLinkBean> getInternalLinkBeans(){
        return Arrays.asList(
                   new OperatorLinkBean(getSelectionNodeID(), getOutputNodeID()),
                   new OperatorLinkBean(getExtractionNodeID(), getSelectionNodeID()),
                   new OperatorLinkBean(getInputNodeID(), getExtractionNodeID())
               );
    }

    /**
     * Return a list of IDs of operators required by this statement (the dependencies of this Statement)
     * when converted to beans.
     * The only required view for a { @code SelectExtractStatement } is the one in the From clause.
     * @return A list with the IDs of required Statements.
     */
    @Override
    public List<String> getInputViews(){
        return Arrays.asList(this.fromClause);
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != this.getClass()) { return false; }
        SelectExtractStatement selectExtractStatement = (SelectExtractStatement) other;
        return new EqualsBuilder()
                    .appendSuper(super.equals(selectExtractStatement))
                    .append(selectPredicate, selectExtractStatement.selectPredicate)
                    .append(extractPredicate, selectExtractStatement.extractPredicate)
                    .append(fromClause, selectExtractStatement.fromClause)
                    .append(limitClause, selectExtractStatement.limitClause)
                    .append(offsetClause, selectExtractStatement.offsetClause)
                    .isEquals();
    }
    
}




