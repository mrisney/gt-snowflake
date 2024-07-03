package org.geotools.data.snowflake;

import java.io.Writer;

import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.Beyond;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
import org.geotools.api.filter.spatial.Within;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;

public class SnowflakeFilterToSQL extends FilterToSQL {
	public SnowflakeFilterToSQL(Writer out) {
		super(out);
	}

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
		capabilities.addType(DWithin.class);
		capabilities.addType(Beyond.class);

		return capabilities;
	}

}
