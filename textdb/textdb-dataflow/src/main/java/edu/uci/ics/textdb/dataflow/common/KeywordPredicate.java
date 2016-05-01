package edu.uci.ics.textdb.dataflow.common;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.StringField;


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

            for (int i=0;i< attributeList.size();i++){
                temp[i] = attributeList.get(i).getFieldName();
            }
            this.fields = temp;
            this.tokens = queryTokenizer(analyzer, this.query);
            this.analyzer = analyzer;
            this.queryObject = createQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public boolean satisfy(ITuple tuple) {
        return true;
    }



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

    public ArrayList<String> queryTokenizer(Analyzer analyzer,  String query) {

        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream  = analyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        try{
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String term = charTermAttribute.toString();
                result.add(term);
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}