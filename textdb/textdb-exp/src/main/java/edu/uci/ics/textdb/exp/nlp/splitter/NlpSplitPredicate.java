package edu.uci.ics.textdb.exp.nlp.splitter;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.international.Language;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.international.arabic.process.ArabicTokenizer;
//import edu.stanford.nlp.international.pennchinese.CHTBTokenizer;
import edu.stanford.nlp.international.french.process.FrenchTokenizer;
import edu.stanford.nlp.international.spanish.process.SpanishTokenizer;

import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NlpSplitPredicate extends PredicateBase {
    
    private final Language nlpLanguage;
    private final NLPOutputType outputType;
    private final String inputAttributeName;
    private final String resultAttributeName;
    //make a variable "outputType" that takes two values, one for one to one transformation
    // and another for one to many transformation with one to many as default
    
    
    public NlpSplitPredicate(
            @JsonProperty(value = PropertyNameConstants.NLP_LANGUAGE, required = true)
            Language nlpLanguage,
            @JsonProperty(value = PropertyNameConstants.NLP_OUTPUT_TYPE, required = true)
            NLPOutputType outputType,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
            ) {
        this.nlpLanguage = nlpLanguage;
        this.outputType = outputType;
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.NLP_LANGUAGE)
    protected Language getLanguage() {
        return this.nlpLanguage;
    }
    
    @JsonProperty(PropertyNameConstants.NLP_OUTPUT_TYPE)
    public NLPOutputType getOutputType() {
        return this.outputType;
    }
    
    protected TokenizerFactory<CoreLabel> getTokenizerFactory() {
        switch(this.nlpLanguage) {
        case Arabic: return ArabicTokenizer.factory();
//        case Chinese: return CHTBTokenizer.factory();
        case French: return FrenchTokenizer.factory();
//        case German: return GermanTokenizer.factory();
//        case Hebrew: return HebrewTokenizer.factory();
        case Spanish: return SpanishTokenizer.factory();
        default: return PTBTokenizer.coreLabelFactory();        
        }

    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }

}
