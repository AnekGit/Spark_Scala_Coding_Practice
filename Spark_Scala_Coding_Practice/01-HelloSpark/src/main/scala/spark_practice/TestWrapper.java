package spark_practice;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * created by ANEK on Friday 8/6/2021 at 4:31 PM
 */

public class TestWrapper implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, String> zap;
    private ArrayList<String> strA;
    private String name;
    private String value;

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
    public Map<String, String> getZap() {
        return zap;
    }
    public void setZap(Map<String, String> zap) {
        this.zap = zap;
    }
    public ArrayList<String> getStrA() {
        return strA;
    }
    public void setStrA(ArrayList<String> strA) {
        this.strA = strA;
    }
}