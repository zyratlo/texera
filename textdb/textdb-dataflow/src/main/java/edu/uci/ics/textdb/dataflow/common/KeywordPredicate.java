package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.utils.Utils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 *  @author prakul
 *
 */

/**
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate{

    private final List<Attribute> attributeList;
    private final String[] fields;
    private final String query;
    private final Query queryObject;
    private ArrayList<String> tokens;
    private Analyzer analyzer;

    public KeywordPredicate(String query, List<Attribute> attributeList, Analyzer analyzer ) throws DataFlowException{
        try {
            this.query = query;
            this.attributeList = attributeList;
            String[] temp = new String[attributeList.size()];

            for (int i=0; i < attributeList.size(); i++){
                temp[i] = attributeList.get(i).getFieldName();
            }
            this.fields = temp;
            this.tokens = Utils.tokenizeQuery(analyzer, this.query);
            this.analyzer = analyzer;
            this.queryObject = createQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public boolean satisfy(ITuple tuple) {

        //This method is necessary for the interface implementation, and it's really not used.
        return true;
    }

    /**
     * Creates a Query object as a boolean Query on all attributes.
     * Example: For creating a query like
     * (TestConstants.DESCRIPTION + ":lin" + " AND " + TestConstants.LAST_NAME + ":lin")
     * we provide a list of AttributeFields (Description, Last_name) to search on and a query string (lin)
     *
     * TODO #88:BooleanQuery() is deprecated. In future a better solution could be worked out in Query builder layer

     * @return QueryObject
     * @throws ParseException
     */
    private Query createQueryObject() throws ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        MultiFieldQueryParser parser = new MultiFieldQueryParser(this.fields, this.analyzer);
        for(String searchToken: this.tokens){
            Query termQuery = parser.parse(searchToken);
            booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
        }
        return booleanQuery;
    }

    public String getQuery(){
        return query;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }
    public Query getQueryObject(){return this.queryObject;}

    public ArrayList<String> getTokens(){return this.tokens;}

    public Analyzer getAnalyzer(){
        return analyzer;
    }


}