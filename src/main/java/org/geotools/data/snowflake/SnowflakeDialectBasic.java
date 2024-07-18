package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.util.factory.Hints;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTWriter;

public class SnowflakeDialectBasic extends BasicSQLDialect {

	// Delegate used to run methods already implemented by SnowflakeDialect
	SnowflakeDialect delegate;
	
	// Constructor method
	public SnowflakeDialectBasic(JDBCDataStore dataStore) {
		super(dataStore);
		delegate = new SnowflakeDialect(dataStore);
	}
	
	// Returns a string representation of Snowflake's name escape character
	@Override
	public String getNameEscape() {
		return delegate.getNameEscape();
	}
	
	// Returns the string representation of the mapped Geographic Type passed to the function
	@Override
	public String getGeometryTypeName(Integer type) {
		return delegate.getGeometryTypeName(type);
	}
	
	// Returns the SRID of the provided geometry column
	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
		return delegate.getGeometrySRID(schemaName, tableName, columnName, cx);
	}
	
	// Uses default implementation of SQLDialect.encodeColumnName()
	@Override
	public void encodeColumnName(String prefix, String raw, StringBuffer sql) {
		delegate.encodeColumnName(prefix, raw, sql);
	}
	
	// Uses default implementation of SQLDialect.encodeColumnName()
	public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, StringBuffer sql) {
		delegate.encodeColumnName(prefix, gatt.getLocalName(), sql);
	}
	
	// Encodes the provided geometry column as Well-Known-Binary and appends it to the SQL Buffer
	@Override
	public void encodeGeometryColumn (GeometryDescriptor gatt, String prefix, int srid, Hints hints, StringBuffer sql) {
		delegate.encodeGeometryColumn(gatt, prefix, srid, hints, sql);
	}
	
	// Uses the default implementation of SQLDialect.encodeColumnType()
	@Override
	public void encodeColumnType(String sqlTypeName, StringBuffer sql) {
		delegate.encodeColumnType(sqlTypeName, sql);
	}
	
	// Creates a mapping between SQL's Geometric classes and the GeoTypes Enum declared in SnowflakeDialect
	@Override
	public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
		delegate.registerClassToSqlMappings(mappings);
	}
	
	// Creates a mapping between the GeoTypes Enum declared in SnowflakeDialect and SQL's Geometric classes
	@Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        delegate.registerSqlTypeToClassMappings(mappings);
    }

	// Creates a mapping of the named string from the GeoTypes Enum declared in SnowflakeDialect and SQL's Geometric classes
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        delegate.registerSqlTypeNameToClassMappings(mappings);
    }
    
    // Creates a mapping to override SQL's types with Snowflake's types
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
        delegate.registerSqlTypeToSqlTypeNameOverrides(overrides);
    }
    
    // Appends code that encodes the provided Geometry value as Well-Known-Text to the SQL Buffer
    @Override
    public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
		
    	if (value != null) {
    		sql.append("ST_GEOMFROMTEXT('");
    		if (srid < 0) srid = 0;
    		
    		sql.append(new WKTWriter().write(value));
    		sql.append("', ").append(srid).append(")");
    		
    	} else {
			sql.append("NULL");
    	}
    }

    // Returns the Well-Known-Binary representation of the provided geometry column
    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column, GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
    	
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
    
    // Appends code for getting the bounding box of the provided geometry column to the SQL Buffer
    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
    	delegate.encodeGeometryEnvelope(tableName, geometryColumn, sql);
    }
    
    // Converts the provided column into Well-Known-Binary and returns the bounding Envelope using the internal implementation from GeoServer
    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
    	return delegate.decodeGeometryEnvelope(rs, column, cx);
    }
    
    // Creates, configures, and returns the instance of SnowflakeFilterToSQL to use with the Snowflake Datastore
    @Override
	public FilterToSQL createFilterToSQL() {
    	
		SnowflakeFilterToSQL fts = new SnowflakeFilterToSQL(new StringWriter());
		fts.setEscapeBackslash(true);
		return fts;
	}
}
