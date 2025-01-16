package org.sujith;

import java.util.Comparator;

public class JsonDataComparator implements Comparator<JsonData> {
    @Override
    public int compare(JsonData o1, JsonData o2){
    return o1.getUserId().compareTo(o2.getUserId());
    }

}
