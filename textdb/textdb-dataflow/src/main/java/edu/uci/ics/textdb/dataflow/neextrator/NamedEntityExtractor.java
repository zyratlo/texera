package edu.uci.ics.textdb.dataflow.neextrator;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * @author Feng [sam0227] on 4/27/16.
 *         <p>
 *         Wrap the Stanford NLP Named Entity Recognizer as an operator.
 *         This operator would recognize 7 classes: Location, Person, Organization, Money, Percent, Date and Time.
 *         Return the recoginized data as a list of spans.
 *         <p>
 *         For example: Given tuple with two field named: sentence1, sentence2.
 *         tuple: ["Google is an organization.", "Its headquarter is in Mountain View."]
 *         return:
 *         ["sentence1,0,6,Google, NE_ORGANIZATION", "sentence2,22,25,Mountain View, NE_LOCATION"]
 */

public class NamedEntityExtractor implements IOperator {


    private IOperator sourceOperator;
    private List<Attribute> searchInAttributes;
    private ITuple sourceTuple;

    public static final String NE_NUMBER = "Number";
    public static final String NE_LOCATION = "Location";
    public static final String NE_PERSON = "Person";
    public static final String NE_ORGANIZATION = "Organization";
    public static final String NE_MONEY = "Money";
    public static final String NE_PERCENT = "Percent";
    public static final String NE_DATE = "Date";
    public static final String NE_TIME = "Time";


    public NamedEntityExtractor(IOperator operator, List<Attribute> searchInAttributes) {
        this.sourceOperator = operator;
        this.searchInAttributes = searchInAttributes;
    }


    /**
     * @about Opens Named Entity Extractor
     */
    @Override
    public void open() throws Exception {
        try {
            sourceOperator.open();
            sourceTuple = sourceOperator.getNextTuple();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }





    /**
     * @about Return all named entities that are recognized in a document.
     * Return format is a Tuple that contains only one field which is
     * a list of spans of the result.
     * @overview First get a tuple from the source operator then process it
     * using the Stanford NLP package. for all recognized words, compute their
     * spans and return all as a list.
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (sourceTuple == null) {
            return null;
        } else {
            List<Span> spanList = new ArrayList<>();
            for (Attribute attribute : searchInAttributes) {
                String fieldName = attribute.getFieldName();
                IField field = sourceTuple.getField(fieldName);
                spanList.addAll(getSpans(field, fieldName));
            }
            IField spanField = new ListField<Span>(spanList);
            List<IField> fields = new ArrayList<IField>();
            fields.add(spanField);
            ITuple resultTuple = new DataTuple(new Schema(SchemaConstants.SPAN_LIST_ATTRIBUTE), fields.toArray(new IField[fields.size()]));
            sourceTuple = sourceOperator.getNextTuple();
            return resultTuple;
        }
    }

    /**
     * This function takes an (TextField) IField and a String (the field's name) as input and use the Stanford NLP package to process the string.
     * It returns a list of spans
     * <p>
     * <p>
     * Not to be confuse:     Value in these spans is the word being extracted while key is the NE_Constant (For example: Location, Person etc)*
     * Description is the name of the IField where the word comes from.
     *
     * @param iField
     * @return a List of spans of the extracted information
     */
    private List<Span> getSpans(IField iField, String fieldName) {
        List<Span> spanList = new ArrayList<>();
        String text = (String) iField.getValue();
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String NLPConstant = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                if (!NLPConstant.equals("O")) {

                    String NEConstant = getNEConstant(NLPConstant);
                    int start = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
                    int end = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);

                    Span span = new Span(fieldName, start, end, NEConstant, word);


                    if (spanList.size() >= 1) {
                        Span previousSpan = spanList.get(spanList.size() - 1);
                        if (previousSpan.getFieldName().equals(span.getFieldName())
                                && (span.getStart()-previousSpan.getEnd() <= 1)
                                && previousSpan.getKey().equals(span.getKey())) {
                            Span newspan = mergeTwoSpan(previousSpan, span);
                            span = newspan;
                            spanList.remove(spanList.size() - 1);
                        }
                    }
                    spanList.add(span);

                }
            }

        }

        return spanList;
    }


    /**
     * This function takes two spans as input and merge then as a new span
     * <p>
     * There are three constraints that the caller need to make sure:
     * 1. The two span are adjancent.That is, the previous span has a end value that is 1 less than the current span.
     * 2. The two span are in the same field. Thus they have the same fieldName
     * 3. The two span have the same key (Organization, Person,... etc)
     *
     * @param previousSpan
     * @param currentSpan
     * @return
     */
    private Span mergeTwoSpan(Span previousSpan, Span currentSpan) {
        String previousWord = previousSpan.getValue();
        String currentWord = currentSpan.getValue();

        String newWord = previousWord + " " + currentWord;

        String NEConstant = previousSpan.getKey();
        String fieldName = previousSpan.getFieldName();
        int start = previousSpan.getStart();
        int end = currentSpan.getEnd();

        Span mergedspan = new Span(fieldName, start, end, NEConstant, newWord);

        return mergedspan;

    }

    /**
     * This function takes an Stanford NLP Constant (The 7 Classes as: LOCATION,PERSON,ORGANIZATION,MONEY,PERCENT,DATE
     * and TIME) and return the corresponding NE Constant that we used in this package.
     *
     * @param NLPConstant
     * @return
     */
    private String getNEConstant(String NLPConstant) {
        String NEConstant;
        switch (NLPConstant) {
            case "NUMBER":
                NEConstant = this.NE_NUMBER;
                break;
            case "LOCATION":
                NEConstant = this.NE_LOCATION;
                break;
            case "PERSON":
                NEConstant = this.NE_PERSON;
                break;
            case "ORGANIZATION":
                NEConstant = this.NE_ORGANIZATION;
                break;
            case "MONEY":
                NEConstant = this.NE_MONEY;
                break;
            case "PERCENT":
                NEConstant = this.NE_PERCENT;
                break;
            case "DATE":
                NEConstant = this.NE_DATE;
                break;
            case "TIME":
                NEConstant = this.NE_TIME;
                break;
            default:
                NEConstant = null;
                break;
        }
        return NEConstant;

    }


    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            searchInAttributes = null;
            sourceTuple = null;
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
