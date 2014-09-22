package edu.berkeley.veloxms;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class BaseItemSet {

    private List<Long> items;

    public BaseItemSet(List<Long> items) {
        this.items = items;
    }

    public BaseItemSet() {
        items = null;
    }

    @JsonProperty
    public List<Long> getItems() {
        return this.items;
    }
    
    @JsonProperty
    public void setItems(List<Long> items) {
        this.items = items;
    }

}
