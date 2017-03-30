package edu.uci.ics.textdb.exp.sampler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;

/**
 * @author Qinhua Huang
 *
 */
public class SamplerPredicate implements IPredicate {
    
    public enum SampleType {
        RANDOM_SAMPLE("random"),

        FIRST_K_ARRIVAL("firstk");
        
        private final String name;
        
        private SampleType(String name) {
            this.name = name;
        }
        
        // use the name string instead of enum string in JSON
        @JsonValue
        public String getName() {
            return this.name;
        }
    }
    
    private final int sampleSize;
    private final SampleType sampleType;
    @JsonCreator
    public SamplerPredicate(
            @JsonProperty(value = PropertyNameConstants.SAMPLE_SIZE, required = true)
            int sampleSize,
            @JsonProperty(value = PropertyNameConstants.SAMPLE_TYPE, required = true)
            SampleType sampleType ) {
        this.sampleSize = sampleSize;
        this.sampleType = sampleType;
    }
    
    @JsonProperty(PropertyNameConstants.SAMPLE_SIZE)
    public double getSampleSize() {
        return sampleSize;
    }
    
    @JsonProperty(PropertyNameConstants.SAMPLE_TYPE)
    public SampleType getSampleType() {
        return sampleType;
    }

}