package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.factory.Hints;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

public class SnowflakeDialect extends SQLDialect {
	
	//public enum GeoTypes {POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON, GEOMETRY, GEOMETRY_COLLECTION};
	
	protected Integer POINT = Integer.valueOf(2001);
	protected Integer MULTIPOINT = Integer.valueOf(2002);
    protected Integer LINESTRING = Integer.valueOf(2003);
    protected Integer MULTILINESTRING = Integer.valueOf(2004);
    protected Integer POLYGON = Integer.valueOf(2005);
    protected Integer MULTIPOLYGON = Integer.valueOf(2006);
    protected Integer GEOMETRY = Integer.valueOf(2007);
    protected Integer GEOMETRY_COLLECTION = Integer.valueOf(2008);
    protected Integer GEOGRAPHY = Integer.valueOf(2009);
	
	private static final Logger LOGGER = Logging.getLogger(SnowflakeDialect.class);
	private static final String CLASS_NAME = "SnowflakeDialect";

	public SnowflakeDialect(JDBCDataStore dataStore) {
		super(dataStore);
		
		LOGGER.entering(CLASS_NAME, "SnowflakeDialect", dataStore);
		LOGGER.exiting(CLASS_NAME, "SnowflakeDialect");
	}
	

	// Added by Austin 7/9/2024
	@Override
	public String getNameEscape() {
		LOGGER.entering(CLASS_NAME, "getNameEscape");
		LOGGER.exiting(CLASS_NAME, "getNameEscape", "\"");
		return "\"";
	}
	
	// Added by Austin 7/9/2024
	@Override
    public String getGeometryTypeName(Integer type) {
		LOGGER.entering(CLASS_NAME, "getGeometryType", type);
		
        if (POINT.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "POINT");
            return "POINT";
        }

        if (MULTIPOINT.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "MULTIPOINT");
            return "MULTIPOINT";
        }

        if (LINESTRING.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "LINESTRING");
            return "LINESTRING";
        }

        if (MULTILINESTRING.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "MULTILINESTRING");
            return "MULTILINESTRING";
        }

        if (POLYGON.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "POLYGON");
            return "POLYGON";
        }

        if (MULTIPOLYGON.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "MULTIPOLYGON");
            return "MULTIPOLYGON";
        }

        if (GEOMETRY_COLLECTION.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "GEOMETRYCOLLECTION");
            return "GEOMETRYCOLLECTION";
        }
        
        if (GEOMETRY.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "GEOMETRY");
        	return "GEOMETRY";
        }
        
        if (GEOGRAPHY.equals(type)) {
        	LOGGER.exiting(CLASS_NAME, "getGeometryType", "GEOGRAPHY");
        	return "GEOGRAPHY";
        }
        
        LOGGER.exiting(CLASS_NAME, "getGeometryType", super.getGeometryTypeName(type));

        return super.getGeometryTypeName(type);
    }
	
	// Changed by Austin 7/9/2024
	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		
		LOGGER.entering(CLASS_NAME, "getGeometrySRID", new Object[] {schemaName, tableName, columnName, cx});
		
		
		
		// Execute SELECT TOP 1 ST_SRID(<columnName>) FROM <tableName>; 
		StringBuffer sql = new StringBuffer();
		
		sql.append("SELECT TOP 1 ST_SRID(");
		encodeColumnName(null, columnName, sql);
		sql.append(") FROM ");
		
		if (schemaName != null) {
			encodeTableName(schemaName, sql);
			sql.append(".");
		}
		
		encodeSchemaName(tableName, sql);
		sql.append(" WHERE ");
		encodeColumnName(null, columnName, sql);
		sql.append(" IS NOT NULL");
		
		
		LOGGER.finer("getGeometrySRID() -- " + sql.toString());
		
		Statement st = cx.createStatement();
		try {
			ResultSet rs = st.executeQuery(sql.toString());
			
			try {
				if (rs.next()) {
					// Return the SRID
					
					LOGGER.finer("getGeometrySRID() -- Successfully retrieved SRID");
					
					
					LOGGER.exiting(CLASS_NAME, "getGeometrySRID", Integer.valueOf(rs.getInt(1)));
					return Integer.valueOf(rs.getInt(1));
				} else {
					// Couldn't find SRID
					
					LOGGER.finer("getGeometrySRID() -- Failed to retrieve SRID");
					
					LOGGER.exiting(CLASS_NAME, "getGeometrySRID", null);
					return null;
				}
			} finally {
				dataStore.closeSafe(rs);
			}
		} finally {
			dataStore.closeSafe(st);
		}
	}

	// Added by Austin 7/9/2024
	@Override
	public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, Hints hints, StringBuffer sql) {
		
		LOGGER.entering(CLASS_NAME, "encodeGeometryColumn", new Object [] { gatt, prefix, srid, hints, sql});
		sql.append("ST_ASWKB(");
		encodeColumnName(prefix, gatt.getLocalName(), sql);
		sql.append(")");
		LOGGER.exiting(CLASS_NAME, "encodeGeometryColumn", sql.toString());
	}
	
	// Changed by Austin 7/9/2024
	@Override
	public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {

		LOGGER.entering(CLASS_NAME, "encodeGeometryEnvelope", new Object[] {tableName, geometryColumn, sql});
		
		
		
		sql.append("ST_ASWKB(ST_ENVELOPE(");
		encodeColumnName(null, geometryColumn, sql);
		sql.append("))");
		
		
				
		LOGGER.exiting(CLASS_NAME,  "encodeGeometryEnvelope", sql.toString());
	}
	
	// Changed by Austin 7/9/2024
	@Override
	public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {

		LOGGER.entering(CLASS_NAME, "decodeGeometryEnvelope", new Object[] {rs, column, cx});
		
		byte[] wkb = rs.getBytes(column);
		
		try {
			Geometry geom = new WKBReader().read(wkb);
			
			LOGGER.exiting(CLASS_NAME, "decodeGeometryEnvelope", geom.getEnvelopeInternal());
			
			return geom.getEnvelopeInternal();
		} catch (ParseException e) {
			String msg = "Error decoding wkb for envelope";
			
			LOGGER.exiting(CLASS_NAME, "decodeGeometryEnvelope", "IOException: " + msg);
			
			throw (IOException) new IOException(msg).initCause(e);
		}
	}
	
	// Changed by Austin 7/9/2024
	@Override
	public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column,
			GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
		
		LOGGER.entering(CLASS_NAME, "decodeGeometryValue", new Object[] {descriptor, rs, column, factory, cx, hints});
		
		byte[] bytes = rs.getBytes(column);
		if (bytes == null) {
			
			LOGGER.exiting(CLASS_NAME, "decodeGeometryValue", null);
			
			return null;
		}

		try {
			
			LOGGER.exiting(CLASS_NAME, "decodeGeometryValue", new WKBReader(factory).read(bytes));
			
			return new WKBReader(factory).read(bytes);
			
		} catch (ParseException e) {
			
			String msg = "Error decoding wkb";
			LOGGER.exiting(CLASS_NAME, "decodeGeometryValue", "IOException: " + msg);
			
			throw (IOException) new IOException(msg).initCause(e);
		}
	}
	
	// Added by Austin 7/9/2024
	@Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);
        
        LOGGER.entering(CLASS_NAME, "registerClassToSqlMappings", mappings);

        mappings.put(Point.class, POINT);
        mappings.put(LineString.class, LINESTRING);
        mappings.put(Polygon.class, POLYGON);
        mappings.put(MultiPoint.class, MULTIPOINT);
        mappings.put(MultiLineString.class, MULTILINESTRING);
        mappings.put(MultiPolygon.class, MULTIPOLYGON);
        mappings.put(Geometry.class, GEOMETRY);
        mappings.put(GeometryCollection.class, GEOMETRY_COLLECTION);
        mappings.put(Geometry.class, GEOGRAPHY);
        
        LOGGER.exiting(CLASS_NAME, "registerClassToSqlMappings", mappings);
        
    }

	// Added by Austin 7/9/2024
    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        super.registerSqlTypeToClassMappings(mappings);
        
        LOGGER.entering(CLASS_NAME, "registerSqlTypeToClassMappings", mappings);

        mappings.put(POINT, Point.class);
        mappings.put(MULTIPOINT, MultiPoint.class);
        mappings.put(LINESTRING, LineString.class);
        mappings.put(MULTILINESTRING, MultiLineString.class);
        mappings.put(POLYGON, Polygon.class);
        mappings.put(MULTIPOLYGON, MultiPolygon.class);
        mappings.put(GEOMETRY, Geometry.class);
        mappings.put(GEOMETRY_COLLECTION, GeometryCollection.class);
        mappings.put(GEOGRAPHY, Geometry.class);
        
        LOGGER.exiting(CLASS_NAME, "registerSqlTypeToClassMappings", mappings);
    }

    // Added by Austin 7/9/2024
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);
        
        LOGGER.entering(CLASS_NAME, "registerSqlTypeNameToClassMappings", mappings);

        mappings.put("POINT", Point.class);
        mappings.put("MULTIPOINT", MultiPoint.class);
        mappings.put("LINESTRING", LineString.class);
        mappings.put("MULTILINESTRING", MultiLineString.class);
        mappings.put("POLYGON", Polygon.class);
        mappings.put("MULTIPOLYGON", MultiPolygon.class);
        mappings.put("GEOMETRY", Geometry.class);
        mappings.put("GEOMETRYCOLLECTION", GeometryCollection.class);
        mappings.put("GEOGRAPHY", Geometry.class);
        
        LOGGER.exiting(CLASS_NAME, "registerSqlTypeNameToClassMappings", mappings);
    }
    
    // Added by Austin 7/9/2024
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
    	
    	LOGGER.entering(CLASS_NAME, "registerSqlTypeToSqlTypeNameOverrides", overrides);
    	
        overrides.put(Types.BOOLEAN, "BOOLEAN");
        overrides.put(Types.CLOB, "STRING");
        
        LOGGER.exiting(CLASS_NAME, "registerSqlTypeToSqlTypeNameOverrides", overrides);
    }
	
	@Override
	public int getGeometryDimension(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {

		LOGGER.entering(CLASS_NAME, "getGeometryDimension", new Object[] {schemaName, tableName, columnName, cx});
		
		// Implementation of getting geometry dimension
		
		LOGGER.exiting(CLASS_NAME, "getGeometryDimension", "Hard-Coded 2");
		
		return 2;
	}
}
