package edu.uci.ics.textdb.exp.projection;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class ProjectionPredicate implements IPredicate {
    
    private final List<String> projectionFields;
    
    @JsonCreator
    public ProjectionPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> projectionFields) {
        this.projectionFields = projectionFields;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getProjectionFields() {
        return new ArrayList<>(projectionFields);
    }
}
