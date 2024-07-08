package org.geotools.data.snowflake;

import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import org.geotools.util.logging.Logging;

public class SnowflakeFilterToSQL extends FilterToSQL {
	
	private static final Logger LOGGER = Logging.getLogger(SnowflakeFilterToSQL.class);
	
	public SnowflakeFilterToSQL(Writer out) {
		super(out);
		LOGGER.log(Level.INFO, "Initialized SnowflakeFilterToSQL()");
	}

	@Override
	protected FilterCapabilities createFilterCapabilities() {
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Start");
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Creating default filter capabilities");
		FilterCapabilities capabilities = super.createFilterCapabilities();
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding BBOX.class filter");
		capabilities.addType(BBOX.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Contains.class filter");
		capabilities.addType(Contains.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Crosses.class filter");
		capabilities.addType(Crosses.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Disjoint.class filter");
		capabilities.addType(Disjoint.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Equals.class filter");
		capabilities.addType(Equals.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Intersects.class filter");
		capabilities.addType(Intersects.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Overlaps.class filter");
		capabilities.addType(Overlaps.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Touches.class filter");
		capabilities.addType(Touches.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Within.class filter");
		capabilities.addType(Within.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding DWithin.class filter");
		capabilities.addType(DWithin.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilities() -- Adding Beyond.class filter");
		capabilities.addType(Beyond.class);
		
		LOGGER.log(Level.INFO, "createFilterCapabilites() -- End");

		return capabilities;
	}

}
