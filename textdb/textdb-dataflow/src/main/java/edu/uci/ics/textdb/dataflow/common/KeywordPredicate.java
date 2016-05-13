package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.ArrayList;
import java.util.List;

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
    private IDataStore dataStore;

    public KeywordPredicate(String query, List<Attribute> attributeList, Analyzer analyzer,IDataStore dataStore ) throws DataFlowException{
        try {
            this.query = query;
            this.tokens = Utils.tokenizeQuery(analyzer, query);
            this.attributeList = attributeList;
            this.dataStore = dataStore;
            String[] temp = new String[attributeList.size()];

            for(int i=0; i < attributeList.size(); i++){
                temp[i] = attributeList.get(i).getFieldName();
            }
            this.fields = temp;
            this.analyzer = analyzer;
            this.queryObject = createLuceneQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * Creates a Query object as a boolean Query on all attributes
     * Example: For creating a query like
     * (TestConstants.DESCRIPTION + ":lin" + " AND " + TestConstants.LAST_NAME + ":lin")
     * we provide a list of AttributeFields (Description, Last_name) to search on and a query string (lin)
     *
     * TODO #88:BooleanQuery() is deprecated. In future a better solution could be worked out in Query builder layer
     *
     * @return Query
     * @throws ParseException
     */
    private Query createLuceneQueryObject() throws ParseException {

        List<String> fieldList = new ArrayList<String>();
        BooleanQuery luceneBooleanQuery = new BooleanQuery();

        for(int i=0; i< attributeList.size(); i++){

            String fieldName = attributeList.get(i).getFieldName();

            /*
            If the field type is String, we need to perform an exact match
            without parsing the query. Hence add them directly to the Query
             */
            if(attributeList.get(i).getFieldType() == FieldType.STRING){
                Query termQuery = new TermQuery(new Term(fieldName, query));
                luceneBooleanQuery.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            else {
                fieldList.add(fieldName);
            }
        }

        if(fieldList.size()==0){
            return luceneBooleanQuery;
        }

        /*
        For all the other fields, parse the query using query parser
        and generate  boolean query
         */
        String[] remainingFields = (String[]) fieldList.toArray(new String[0]);
        BooleanQuery queryOnOtherFields = new BooleanQuery();
        MultiFieldQueryParser parser = new MultiFieldQueryParser(remainingFields, analyzer);

        for(String searchToken: this.tokens){
            Query termQuery = parser.parse(searchToken);
            queryOnOtherFields.add(termQuery, BooleanClause.Occur.MUST);
        }

        /*
        Merge the query for non-String fields with the StringField Query
         */
        luceneBooleanQuery.add(queryOnOtherFields,BooleanClause.Occur.SHOULD);
        return luceneBooleanQuery;
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

    public DataReaderPredicate convertToDataReaderPredicate() {
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.dataStore,this.queryObject);
        return dataReaderPredicate;
    }


}