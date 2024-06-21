package org.geotools.data.snowflake;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class SnowflakeSQLDialect extends BasicSQLDialect {

    public SnowflakeSQLDialect(JDBCDataStore dataStore) {
        super(dataStore);
    }

    @Override
    public boolean includeTable(String schemaName, String tableName, Connection cx)
            throws SQLException {
        // Customize as needed for Snowflake
        return true;
    }

    // @Override
    public String getName() {
        return "Snowflake";
    }

    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        super.initializeConnection(cx);
        // Add any Snowflake-specific initialization if needed
    }

    @Override
    public void encodeGeometryColumn(
            GeometryDescriptor descriptor, String prefix, int srid, Hints hints, StringBuffer sql) {
        // Implement Snowflake-specific geometry encoding if needed
        super.encodeGeometryColumn(descriptor, prefix, srid, hints, sql);
    }

    @Override
    public Integer getGeometrySRID(
            String schemaName, String tableName, String columnName, Connection cx)
            throws SQLException {
        // Implement Snowflake-specific SRID extraction if needed
        return super.getGeometrySRID(schemaName, tableName, columnName, cx);
    }

    @Override
    public Geometry decodeGeometryValue(
            GeometryDescriptor descriptor,
            ResultSet rs,
            String column,
            GeometryFactory factory,
            Connection cx,
            Hints hints)
            throws SQLException {
        // Read the WKT string from the ResultSet
        String wkt = rs.getString(column);
        if (wkt == null) {
            return null;
        }

        // Convert the WKT string to a Geometry object
        WKTReader reader = new WKTReader(factory);
        try {
            return reader.read(wkt);
        } catch (ParseException e) {
            throw new SQLException("Failed to parse WKT for geometry column: " + column, e);
        }
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx)
            throws SQLException {
        // Read the envelope WKT string from the ResultSet
        String envelopeWKT = rs.getString(column);
        if (envelopeWKT == null) {
            return null;
        }

        // Convert the WKT string to an Envelope object
        WKTReader reader = new WKTReader();
        try {
            Polygon envelopePolygon = (Polygon) reader.read(envelopeWKT);
            return envelopePolygon.getEnvelopeInternal();
        } catch (ParseException e) {
            throw new SQLException("Failed to parse WKT for envelope column: " + column, e);
        }
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        // Generate SQL for calculating the envelope of the geometry column
        sql.append("SELECT ST_AsText(ST_Envelope(")
                .append(geometryColumn)
                .append(")) AS envelope FROM ")
                .append(tableName);
    }

    @Override
    public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql)
            throws IOException {
        // TODO Auto-generated method stub

    }
}
