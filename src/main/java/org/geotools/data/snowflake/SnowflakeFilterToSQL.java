package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.Beyond;
import org.geotools.api.filter.spatial.BinarySpatialOperator;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.DistanceBufferOperator;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
import org.geotools.api.filter.spatial.Within;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;

public class SnowflakeFilterToSQL extends FilterToSQL {
	
	private static final Logger LOGGER = Logging.getLogger(SnowflakeFilterToSQL.class);
	private static final String CLASS_NAME = "SnowflakeFilterToSQL";
	
	public SnowflakeFilterToSQL(Writer out) {
		super(out);
		LOGGER.entering(CLASS_NAME, "SnowflakeFilterToSQL");
		LOGGER.exiting(CLASS_NAME, "SnowflakeFilterToSQL");
	}

	@Override
	protected FilterCapabilities createFilterCapabilities() {
		LOGGER.entering(CLASS_NAME, "createFilterCapabilities");
		
		
		LOGGER.finest("createFilterCapabilities() -- Creating default filter capabilities");
		FilterCapabilities capabilities = super.createFilterCapabilities();
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding BBOX.class filter");
		capabilities.addType(BBOX.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Contains.class filter");
		capabilities.addType(Contains.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Crosses.class filter");
		capabilities.addType(Crosses.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Disjoint.class filter");
		capabilities.addType(Disjoint.class);
		
		
		//LOGGER.finer("createFilterCapabilities() -- Adding Equals.class filter");
		//capabilities.addType(Equals.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Intersects.class filter");
		capabilities.addType(Intersects.class);
		
		
		//LOGGER.finer("createFilterCapabilities() -- Adding Overlaps.class filter");
		//capabilities.addType(Overlaps.class);
		
		
		//LOGGER.finer("createFilterCapabilities() -- Adding Touches.class filter");
		//capabilities.addType(Touches.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Within.class filter");
		capabilities.addType(Within.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding DWithin.class filter");
		capabilities.addType(DWithin.class);
		
		
		LOGGER.finer("createFilterCapabilities() -- Adding Beyond.class filter");
		capabilities.addType(Beyond.class);
		
		LOGGER.exiting(CLASS_NAME, "createFilterCapabilities", capabilities);

		return capabilities;
	}
	
	@Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
		LOGGER.entering(CLASS_NAME, "visitLiteralGeometry", expression);
		
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        if (g instanceof LinearRing) {
            // WKT does not support linear rings
        	LOGGER.finest("Geometry is a LinearRing, trying to convert to LineString");
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        }
       
        out.write("ST_GEOMFROMTEXT('" + g.toText() + "', " + currentSRID + ")");
        
        LOGGER.exiting(CLASS_NAME, "visitLiteralGeometry", "ST_GEOMFROMTEXT('" + g.toText() + "', " + currentSRID + ")");
    }
	
	@Override
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            PropertyName property,
            Literal geometry,
            boolean swapped,
            Object extraData) {
		LOGGER.entering(CLASS_NAME, "visitBinarySpatialOperator", new Object[] {filter, property, geometry, swapped, extraData});
		
		LOGGER.exiting(CLASS_NAME, "visitBinarySpatialOperator(BinarySpatialOperator, PropertyName, Literal, boolean, Object)");
        return visitBinarySpatialOperator(filter, property, (Expression) geometry, swapped, extraData);
    }
	
	@Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData) {
		LOGGER.entering(CLASS_NAME, "visitBinarySpatialOperator", new Object[] {filter, e1, e2, extraData});
		LOGGER.exiting(CLASS_NAME, "visitBinarySpatialOperator(BinarySpatialOperator, Expression, Expression, Object)");
        return visitBinarySpatialOperator(filter, e1, e2, false, extraData);
    }
	
	protected Object visitBinarySpatialOperator (BinarySpatialOperator filter, Expression e1, Expression e2, boolean swapped, Object extraData) {
		LOGGER.entering(CLASS_NAME, "visitBinarySpatialOperator", new Object[] {filter, e1, e2, swapped, extraData});
		try {
			if (!(filter instanceof Disjoint) && !(filter instanceof DistanceBufferOperator)) {
                
                out.write("ST_INTERSECTS(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(") = 1");

                if (!(filter instanceof BBOX)) {
                    out.write(" AND ");
                }
            }

            if (filter instanceof BBOX) {
                // nothing to do. already encoded above
                return extraData;
            }
			
			if (filter instanceof DistanceBufferOperator) {
				LOGGER.finest("Filtering for DistanceBufferOperator");
				out.write("ST_DISTANCE(");
				e1.accept(this, extraData);
				out.write(", ");
				e2.accept(this, extraData);
				out.write(")");
				
				if (filter instanceof DWithin) {
					LOGGER.finest("Filtering for DWithin");
					out.write("<");
				} else if (filter instanceof Beyond) {
					LOGGER.finest("Filtering for Beyond");
					out.write(">");
				} else {
					LOGGER.warning("Unknown distance operator");
					throw new RuntimeException("Unknown distance operator");
				}
				out.write(Double.toString(((DistanceBufferOperator) filter).getDistance()));
			} else {
				
				if (filter instanceof Contains) {
					LOGGER.finest("Filtering for Contains");
                    out.write("ST_CONTAINS(");
                } else if (filter instanceof Crosses) {
                	LOGGER.finest("Filtering for Crosses");
                    out.write("ST_INTERSECTS(");
                } else if (filter instanceof Disjoint) {
                	LOGGER.finest("Filtering for Disjoint");
                    out.write("ST_DISJOINT(");
                //} else if (filter instanceof Equals) {
                //    out.write("ST_Equals(");
                } else if (filter instanceof Intersects) {
                	LOGGER.finest("Filtering for Intersects");
                    out.write("ST_INTERSECTS(");
                //} else if (filter instanceof Overlaps) {
                //    out.write("ST_Overlaps(");
                //} else if (filter instanceof Touches) {
                //    out.write("ST_Touches(");
                } else if (filter instanceof Within) {
                	LOGGER.finest("Filtering for Within");
                    out.write("ST_WITHIN(");
                } else {
                	LOGGER.warning("Unknown operator: " + filter);
                    throw new RuntimeException("Unknown operator: " + filter);
                }
				
				LOGGER.finest("Are the expressions swapped? " + (swapped ? "Yes" : "No"));
				if (swapped) {
					LOGGER.finest("Swapping expressions before writing");
                    e2.accept(this, extraData);
                    out.write(", ");
                    e1.accept(this, extraData);
                } else {
                	LOGGER.finest("Writing expressions");
                    e1.accept(this, extraData);
                    out.write(", ");
                    e2.accept(this, extraData);
                }

                out.write(")");
			}
		} catch (IOException e) {
			LOGGER.warning("IOException : " + e.getMessage());
			throw new RuntimeException(e);
		}
		
		LOGGER.exiting(CLASS_NAME, "visitBinarySpatialOperator", extraData);
		
		return extraData;
	}

}
