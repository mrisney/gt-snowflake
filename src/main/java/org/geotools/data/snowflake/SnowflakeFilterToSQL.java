package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.Writer;
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
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Within;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;

public class SnowflakeFilterToSQL extends FilterToSQL {

    private static final Logger LOGGER = Logging.getLogger(SnowflakeFilterToSQL.class);

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
        capabilities.addType(Intersects.class);
        capabilities.addType(Within.class);
        capabilities.addType(DWithin.class);
        capabilities.addType(Beyond.class);

        return capabilities;
    }

    @Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
      
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        if (g instanceof LinearRing) {
            // WKT does not support linear rings
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        }

        out.write("ST_GEOGFROMTEXT('" + g.toText() + "')");

    }

    @Override
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            PropertyName property,
            Literal geometry,
            boolean swapped,
            Object extraData) {
        return visitBinarySpatialOperator(filter, property, (Expression) geometry, swapped, extraData);
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData) {
        return visitBinarySpatialOperator(filter, e1, e2, false, extraData);
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, boolean swapped, Object extraData) {
        try {
            if (!(filter instanceof Disjoint) && !(filter instanceof DistanceBufferOperator)) {
                out.write("ST_INTERSECTS(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(")");

                if (!(filter instanceof BBOX)) {
                    out.write(" = 1");
                }
            }

            if (filter instanceof BBOX) {
                // nothing to do. already encoded above
                return extraData;
            }

            if (filter instanceof DistanceBufferOperator) {
                out.write("ST_DISTANCE(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(")");

                if (filter instanceof DWithin) {
                    out.write("<");
                } else if (filter instanceof Beyond) {
                    out.write(">");
                } else {
                    throw new RuntimeException("Unknown distance operator");
                }
                out.write(Double.toString(((DistanceBufferOperator) filter).getDistance()));
            } else {
                if (filter instanceof Contains) {
                     out.write("ST_CONTAINS(");
                } else if (filter instanceof Crosses) {
                    out.write("ST_INTERSECTS(");
                } else if (filter instanceof Disjoint) {
                    out.write("ST_DISJOINT(");
                } else if (filter instanceof Intersects) {
                    out.write("ST_INTERSECTS(");
                } else if (filter instanceof Within) {
                    out.write("ST_WITHIN(");
                } else {
                    throw new RuntimeException("Unknown operator: " + filter);
                }

                if (swapped) {
                     e2.accept(this, extraData);
                    out.write(", ");
                    e1.accept(this, extraData);
                } else {
                    e1.accept(this, extraData);
                    out.write(", ");
                    e2.accept(this, extraData);
                }

                out.write(")");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return extraData;
    }

    @Override
    public Object visit(Intersects filter, Object extraData) {
        try {
            out.write("ST_INTERSECTS(");
            out.write("ST_GEOGFROMTEXT(");
            filter.getExpression1().accept(this, extraData);
            out.write("), ST_GEOGFROMTEXT(");
            filter.getExpression2().accept(this, extraData);
            out.write("))");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return extraData;
    }

    @Override
    public Object visit(BBOX filter, Object extraData) {
        try {
            out.write("ST_INTERSECTS(");
            out.write("ST_GEOGFROMTEXT('");
            out.write(bboxToWkt(filter));
            out.write("')");
            out.write(", ");
            filter.getExpression1().accept(this, extraData);
            out.write(")");
        } catch (IOException e) {
            LOGGER.warning("IOException while writing SQL: " + e.getMessage());
            throw new RuntimeException(e);
        }
        return extraData;
    }

    private String bboxToWkt(BBOX filter) {
        try {
            org.geotools.geometry.jts.ReferencedEnvelope envelope = new org.geotools.geometry.jts.ReferencedEnvelope(filter.getBounds());
            double minX = envelope.getMinX();
            double minY = envelope.getMinY();
            double maxX = envelope.getMaxX();
            double maxY = envelope.getMaxY();

            return "POLYGON((" + minX + " " + minY + ", "
                    + maxX + " " + minY + ", "
                    + maxX + " " + maxY + ", "
                    + minX + " " + maxY + ", "
                    + minX + " " + minY + "))";
        } catch (Exception e) {
            LOGGER.warning("Error generating WKT for BBOX: " + e.getMessage());
            throw new RuntimeException("Error generating WKT for BBOX", e);
        }
    }
}
