package edu.uci.ics.textdb.dataflow.nlpextrator;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;

public class NlpPredicate implements IPredicate {
    
    /**
     * Named Entity Token Types: NE_ALL, Number, Location, Person, Organization,
     * Money, Percent, Date, Time. Part Of Speech Token Types: Noun, Verb,
     * Adjective, Adverb
     */
    public enum NlpTokenType {
        Noun, Verb, Adjective, Adverb,

        NE_ALL, Number, Location, Person, Organization, Money, Percent, Date, Time;
    }

    private NlpTokenType nlpTokenType;
    private List<Attribute> attributeList;

    public NlpPredicate(NlpTokenType nlpTokenType, List<Attribute> attributeList) {
        this.nlpTokenType = nlpTokenType;
        this.attributeList = attributeList;
    }

    public NlpTokenType getNlpTokenType() {
        return nlpTokenType;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }
    
    public String getNlpTypeIndicator() {
        if (isPOSTokenType(nlpTokenType)) {
            return "POS";
        } else {
            return "NE_ALL";
        }
    }
    
    private static boolean isPOSTokenType(NlpTokenType tokenType) {
        if (tokenType.equals(NlpTokenType.Adjective) || tokenType.equals(NlpTokenType.Adverb)
                || tokenType.equals(NlpTokenType.Noun) || tokenType.equals(NlpTokenType.Verb)) {
            return true;
        } else {
            return false;
        }
    }
}
