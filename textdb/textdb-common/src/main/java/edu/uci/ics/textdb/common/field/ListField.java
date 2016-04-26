package edu.uci.ics.textdb.common.field;

import java.util.List;

import edu.uci.ics.textdb.api.common.IField;

public class ListField<T> implements IField{

    private List<T> list;
    
    public ListField(List<T> list){
        this.list = list;
    }
    
    @Override
    public List<T> getValue() {
        return list;
    }

}
