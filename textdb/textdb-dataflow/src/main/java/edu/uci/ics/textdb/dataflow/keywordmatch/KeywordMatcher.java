package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;

/**
 *  @author prakul
 *
 */
public class KeywordMatcher implements IOperator {
    private final KeywordPredicate predicate;
    private ISourceOperator sourceOperator;
    private ArrayList<Pattern> tokenPatternList;
    private List<Span> spanList;
    private  String query;
    private  List<Attribute> attributeList;
    private  ArrayList<String> queryTokens;


    public KeywordMatcher(IPredicate predicate, ISourceOperator sourceOperator) {
        this.predicate = (KeywordPredicate)predicate;
        this.sourceOperator = sourceOperator;
    }

    @Override
    public void open() throws DataFlowException {
        try {
            Pattern pattern;
            String regex;

            sourceOperator.open();
            query = predicate.getQuery();
            attributeList = predicate.getAttributeList();
            queryTokens = predicate.getTokens();
            tokenPatternList = new ArrayList<Pattern>();
            for(String token : queryTokens){
                regex = "\\b" + token.toLowerCase() + "\\b";
                pattern = Pattern.compile(regex);
                tokenPatternList.add(pattern);
            }

            spanList = new ArrayList<>();

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {

        Schema spanSchema = null;
        String fieldName;
        List<IField> fieldList;
        boolean foundFlag = false;
        boolean schemaDefined = false;
        int positionIndex = 0; // Next position in the field to be checked.
        int spanIndexValue; // Starting position of the matched query

        try {

            ITuple sourceTuple = sourceOperator.getNextTuple();
            if(sourceTuple == null){
                return null;
            }
            fieldList = sourceTuple.getFields();
            spanList.clear();
            if(!schemaDefined){
                schemaDefined = true;
                Schema schema = sourceTuple.getSchema();
                spanSchema = Utils.createSpanSchema(schema);
            }

            for(int attributeIndex = 0; attributeIndex < attributeList.size(); attributeIndex++){
                IField field = sourceTuple.getField(attributeList.get(attributeIndex).getFieldName());
                String fieldValue = (String) (field).getValue();
                if(field instanceof StringField){
                    //Keyword should match fieldValue entirely

                    if(fieldValue.equals(query.toLowerCase())){
                        spanIndexValue = 0;
                        positionIndex = query.length();
                        fieldName = attributeList.get(attributeIndex).getFieldName();
                        addSpanToSpanList(fieldName, spanIndexValue, positionIndex, query, fieldValue);
                        foundFlag = true;
                    }
                }
                else if(field instanceof TextField) {
                    String documentValue;
                    Matcher matcher;
                    //Each element of Array of keywords is matched in tokenized TextField Value
                    for(int iter = 0; iter < queryTokens.size(); iter++) {
                        positionIndex = 0;
                        String query = queryTokens.get(iter);
                        //Ex: For keyword lin it obtains pattern like /blin/b which matches keywords at boundary
                        Pattern pattern = tokenPatternList.get(iter);
                        matcher = pattern.matcher(fieldValue.toLowerCase());
                        while (matcher.find(positionIndex) != false) {
                            spanIndexValue = matcher.start();
                            positionIndex = spanIndexValue + queryTokens.get(iter).length();
                            documentValue = fieldValue.substring(spanIndexValue, positionIndex);
                            fieldName = attributeList.get(attributeIndex).getFieldName();
                            addSpanToSpanList(fieldName, spanIndexValue, positionIndex, query, documentValue);
                            foundFlag = true;

                        }
                    }
                }
                positionIndex = 0;
            }

            //If all the 'attributes to be searched' have been processed return the result tuple with span info
            if (foundFlag){
                foundFlag = false;
                positionIndex = 0;
                return Utils.getSpanTuple(fieldList, spanList, spanSchema);
            }
            //Search next document if the required predicate did not match previous document
            else{
                positionIndex = 0;
                spanList.clear();

                return getNextTuple();

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }

    private void addSpanToSpanList(String fieldName, int start, int end, String key, String value) {
        Span span = new Span(fieldName, start, end, key, value);
        spanList.add(span);
    }


    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}