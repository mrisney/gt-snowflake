package org.geotools.data.snowflake;

import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;

public class SnowflakeFilterToSQL extends FilterToSQL {

	   @Override
	    protected FilterCapabilities createFilterCapabilities() {
	       
	        FilterCapabilities capabilities = super.createFilterCapabilities();
	        capabilities.addType(BBOX.class);
	        capabilities.addType(Contains.class);
	        capabilities.addType(Crosses.class);
	        capabilities.addType(Disjoint.class);
	        capabilities.addType(Equals.class);
	        capabilities.addType(Intersects.class);
	        capabilities.addType(Overlaps.class);
	        capabilities.addType(Touches.class);
	        capabilities.addType(Within.class);
	        capabilities.addType(Beyond.class);

	        return capabilities;
	    }
	   
}
