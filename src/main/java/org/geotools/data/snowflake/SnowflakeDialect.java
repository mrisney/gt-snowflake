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
	
	// Enum for Geographic types to map to SQL
	public static enum GeoTypes {POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON, GEOMETRY, GEOMETRY_COLLECTION, GEOGRAPHY};
	
//	  protected Integer POINT = Integer.valueOf(2001);
//	  protected Integer MULTIPOINT = Integer.valueOf(2002);
//    protected Integer LINESTRING = Integer.valueOf(2003);
//    protected Integer MULTILINESTRING = Integer.valueOf(2004);
//    protected Integer POLYGON = Integer.valueOf(2005);
//    protected Integer MULTIPOLYGON = Integer.valueOf(2006);
//    protected Integer GEOMETRY = Integer.valueOf(2007);
//    protected Integer GEOMETRY_COLLECTION = Integer.valueOf(2008);
//    protected Integer GEOGRAPHY = Integer.valueOf(2009);

	// Constructor method
	public SnowflakeDialect(JDBCDataStore dataStore) {
		super(dataStore);
	}
	

	// Returns a string representation of Snowflake's name escape character
	@Override
	public String getNameEscape() {
		return "\"";
	}
	
	// Returns the string representation of the mapped Geographic Type passed to the function
	@Override
    public String getGeometryTypeName(Integer type) {
		
		GeoTypes selectedType = GeoTypes.values()[type];
		
		switch(selectedType) {
			case POINT:
				return "POINT";
			case MULTIPOINT:
				return "MULTIPOINT";
			case LINESTRING:
				return "LINESTRING";
			case MULTILINESTRING:
				return "MULTILINESTRING";
			case POLYGON:
				return "POLYGON";
			case MULTIPOLYGON:
				return "MULTIPOLYGON";
			case GEOMETRY_COLLECTION:
				return "GEOMETRY_COLLECTION";
			case GEOMETRY:
				return "GEOMETRY";
			case GEOGRAPHY:
				return "GEOGRAPHY";
			default:
				return super.getGeometryTypeName(type);
		}
    }
	
	// Returns the SRID of the provided geometry column
	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		
		// Execute SELECT TOP 1 ST_SRID(<columnName>) FROM <tableName> WHERE <columnName> IS NOT NULL; 
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
		
		
		System.out.println("getGeometrySRID() -- " + sql.toString());
		
		Statement st = cx.createStatement();
		try {
			ResultSet rs = st.executeQuery(sql.toString());
			
			try {
				if (rs.next()) {
					// Return the SRID
					return Integer.valueOf(rs.getInt(1));
				} else {
					// Couldn't find SRID
					System.out.println("Couldn't find SRID");
					return null;
				}
			} finally {
				dataStore.closeSafe(rs);
			}
		} finally {
			dataStore.closeSafe(st);
		}
	}

	// Encodes the provided geometry column as Well-Known-Binary and appends it to the SQL Buffer
	@Override
	public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, Hints hints, StringBuffer sql) {
		
		sql.append("ST_ASWKB(");
		encodeColumnName(prefix, gatt.getLocalName(), sql);
		sql.append(")");
	}
	
	// Appends code for getting the bounding box of the provided geometry column to the SQL Buffer
	@Override
	public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {

		sql.append("ST_ASWKB(ST_ENVELOPE(");
		encodeColumnName(null, geometryColumn, sql);
		sql.append("))");
	}
	
	// Converts the provided column into Well-Known-Binary and returns the bounding Envelope using the internal implementation from GeoServer
	@Override
	public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
		
		byte[] wkb = rs.getBytes(column);
		
		try {
			Geometry geom = new WKBReader().read(wkb);
			
			return geom.getEnvelopeInternal();
		} catch (ParseException e) {
			throw (IOException) new IOException("Error decoding wkb for envelope").initCause(e);
		}
	}
	
	// Returns the Well-Known-Binary representation of the provided geometry column
	@Override
	public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column,
			GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
		
		byte[] bytes = rs.getBytes(column);
		
		if (bytes == null) {
			return null;
		}

		try {
			return new WKBReader(factory).read(bytes);
		} catch (ParseException e) {
			throw (IOException) new IOException("Error decoding wkb").initCause(e);
		}
	}
	
	// Creates a mapping between SQL's Geometric classes and the GeoTypes Enum declared in this class
	@Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);

        mappings.put(Point.class, GeoTypes.POINT.ordinal());
        mappings.put(MultiPoint.class, GeoTypes.MULTIPOINT.ordinal());
        mappings.put(LineString.class, GeoTypes.LINESTRING.ordinal());
        mappings.put(MultiLineString.class, GeoTypes.MULTILINESTRING.ordinal());
        mappings.put(Polygon.class, GeoTypes.POLYGON.ordinal());
        mappings.put(MultiPolygon.class, GeoTypes.MULTIPOLYGON.ordinal());
        mappings.put(Geometry.class, GeoTypes.GEOMETRY.ordinal());
        mappings.put(GeometryCollection.class, GeoTypes.GEOMETRY_COLLECTION.ordinal());
        mappings.put(Geometry.class, GeoTypes.GEOGRAPHY.ordinal());
    }

	// Creates a mapping between the GeoTypes Enum declared in this class and SQL's Geometric classes
    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        super.registerSqlTypeToClassMappings(mappings);

        mappings.put(GeoTypes.POINT.ordinal(), Point.class);
        mappings.put(GeoTypes.MULTIPOINT.ordinal(), MultiPoint.class);
        mappings.put(GeoTypes.LINESTRING.ordinal(), LineString.class);
        mappings.put(GeoTypes.MULTILINESTRING.ordinal(), MultiLineString.class);
        mappings.put(GeoTypes.POLYGON.ordinal(), Polygon.class);
        mappings.put(GeoTypes.MULTIPOLYGON.ordinal(), MultiPolygon.class);
        mappings.put(GeoTypes.GEOMETRY.ordinal(), Geometry.class);
        mappings.put(GeoTypes.GEOMETRY_COLLECTION.ordinal(), GeometryCollection.class);
        mappings.put(GeoTypes.GEOGRAPHY.ordinal(), Geometry.class);
    }

    // Creates a mapping of the named string from the GeoTypes Enum declared in this class and SQL's Geometric classes
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);

        mappings.put("POINT", Point.class);
        mappings.put("MULTIPOINT", MultiPoint.class);
        mappings.put("LINESTRING", LineString.class);
        mappings.put("MULTILINESTRING", MultiLineString.class);
        mappings.put("POLYGON", Polygon.class);
        mappings.put("MULTIPOLYGON", MultiPolygon.class);
        mappings.put("GEOMETRY", Geometry.class);
        mappings.put("GEOMETRYCOLLECTION", GeometryCollection.class);
        mappings.put("GEOGRAPHY", Geometry.class);
    }
    
    // Creates a mapping to override SQL's types with Snowflake's types
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
    	
        overrides.put(Types.BOOLEAN, "BOOLEAN");
        overrides.put(Types.CLOB, "STRING");
    }
	
    // This project currently only supports 2-D Geometric values
	@Override
	public int getGeometryDimension(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		return 2;
	}
}
