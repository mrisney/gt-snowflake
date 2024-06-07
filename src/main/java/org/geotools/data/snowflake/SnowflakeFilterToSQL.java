package org.geotools.data.snowflake;

import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;

public class SnowflakeFilterToSQL extends FilterToSQL {

    public SnowflakeFilterToSQL() {}

    @Override
    protected FilterCapabilities createFilterCapabilities() {

        FilterCapabilities capabilities = super.createFilterCapabilities();

        return capabilities;
    }
}
