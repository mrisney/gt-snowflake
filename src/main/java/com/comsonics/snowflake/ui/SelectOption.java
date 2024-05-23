package com.comsonics.snowflake.ui;

import java.io.Serializable;

public class SelectOption implements Serializable {

    private static final long serialVersionUID = 1L;
    private String key = null;
    private String value = null;

    public SelectOption(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
