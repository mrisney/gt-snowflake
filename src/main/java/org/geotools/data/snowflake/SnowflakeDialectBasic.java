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
	
	private static final Logger LOGGER = Logging.getLogger(SnowflakeDialectBasic.class);
	private static final String CLASS_NAME = "SnowflakeDialectBasic";

	SnowflakeDialect delegate;
	
	public SnowflakeDialectBasic(JDBCDataStore dataStore) {
		super(dataStore);
		String methodName = "SnowflakeDialectBasic";
		
		LOGGER.entering(CLASS_NAME, methodName, dataStore);
		
		delegate = new SnowflakeDialect(dataStore);
		
		LOGGER.exiting(CLASS_NAME, methodName, dataStore);
	}
	
	@Override
	public String getNameEscape() {
		String methodName = "getNameEscape";
		LOGGER.entering(CLASS_NAME, methodName);
		LOGGER.exiting(CLASS_NAME, methodName, delegate.getNameEscape());
		return delegate.getNameEscape();
	}
	
	@Override
	public String getGeometryTypeName(Integer type) {
		String methodName = "getGeometryTypeName";
		LOGGER.entering(CLASS_NAME, methodName, type);
		LOGGER.exiting(CLASS_NAME, methodName, delegate.getGeometryTypeName(type));
		return delegate.getGeometryTypeName(type);
	}
	
	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
		String methodName = "getGeometrySRID";
		LOGGER.entering(CLASS_NAME, methodName, new Object[] {schemaName, tableName, columnName, cx});
		LOGGER.exiting(CLASS_NAME, methodName, delegate.getGeometrySRID(schemaName, tableName, columnName, cx));
		return delegate.getGeometrySRID(schemaName, tableName, columnName, cx);
	}
	
	@Override
	public void encodeColumnName(String prefix, String raw, StringBuffer sql) {
		String methodName = "encodeColumnName";
		LOGGER.entering(CLASS_NAME, methodName, new Object[] {prefix, raw, sql});
		
		delegate.encodeColumnName(prefix, raw, sql);
		
		LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
	}
	
	public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, StringBuffer sql) {
		String methodName = "encodeGeometryColumn";
		LOGGER.entering(CLASS_NAME, methodName, new Object[] {gatt, prefix, srid, sql});
		
		delegate.encodeColumnName(prefix, gatt.getLocalName(), sql);
		
		LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
	}
	
	@Override
	public void encodeGeometryColumn (GeometryDescriptor gatt, String prefix, int srid, Hints hints, StringBuffer sql) {
		String methodName = "encodeGeometryColumn";
		LOGGER.entering(CLASS_NAME, methodName, new Object[] {gatt, prefix, srid, hints, sql});
		
		delegate.encodeGeometryColumn(gatt, prefix, srid, hints, sql);
		
		LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
	}
	
	@Override
	public void encodeColumnType(String sqlTypeName, StringBuffer sql) {
		String methodName = "encodeColumnType";
		LOGGER.entering(CLASS_NAME, methodName, new Object[] {sqlTypeName, sql});
		
		delegate.encodeColumnType(sqlTypeName, sql);
		
		LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
	}
	
	@Override
	public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
		String methodName = "registerClassToSqlMappings";
		LOGGER.entering(CLASS_NAME, methodName, mappings);
		
		delegate.registerClassToSqlMappings(mappings);
		
		LOGGER.exiting(CLASS_NAME, methodName, mappings);
	}
	
	@Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
		String methodName = "registerSqlTypeToClassMappings";
		LOGGER.entering(CLASS_NAME, methodName, mappings);
		
        delegate.registerSqlTypeToClassMappings(mappings);
        
        LOGGER.exiting(CLASS_NAME, methodName, mappings);
    }

    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
    	String methodName = "registerSqlTypeNameToClassMappings";
    	LOGGER.entering(CLASS_NAME, methodName, mappings);
    	
        delegate.registerSqlTypeNameToClassMappings(mappings);
        
        LOGGER.exiting(CLASS_NAME, methodName, mappings);
    }
    
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
    	String methodName = "registerSqlTypeToSqlTypeNameOverrides";
    	LOGGER.entering(CLASS_NAME, methodName, overrides);
    	
        delegate.registerSqlTypeToSqlTypeNameOverrides(overrides);
        
        LOGGER.exiting(CLASS_NAME, methodName, overrides);
        
    }
    
    @Override
    public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
    	String methodName = "encodeGeometryValue";
    	LOGGER.entering(CLASS_NAME, methodName, new Object[] {value, dimension, srid, sql});
		
    	if (value != null) {
    		LOGGER.finer("WKT: " + new WKTWriter().write(value));
    		sql.append("ST_GEOMFROMTEXT('");
    		if (srid < 0) srid = 0;
    		
    		sql.append(new WKTWriter().write(value));
    		sql.append("', ").append(srid).append(")");
    		
    	} else {
			LOGGER.finer("Geometry value is NULL");
			
			sql.append("NULL");
    	}
    	
    	LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
    }

    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column, GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
    	String methodName = "decodeGeometryValue";
    	LOGGER.entering(CLASS_NAME, methodName, new Object[] {descriptor, rs, column, factory, cx, hints});
    	
    	byte[] bytes = rs.getBytes(column);
    	if (bytes == null) {
    		
    		LOGGER.exiting(CLASS_NAME, methodName, null);
    		return null;
    	}
    	
    	try {
    		
    		LOGGER.exiting(CLASS_NAME, methodName, new WKBReader(factory).read(bytes));
    		return new WKBReader(factory).read(bytes);
    	} catch (ParseException e) {
    		String msg = "Error decoding wkb";
    		
    		LOGGER.exiting(CLASS_NAME, methodName, "IOException: " + msg);
    		throw (IOException) new IOException(msg).initCause(e);
    	}
    }
    
    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
    	String methodName = "encodeGeometryEnvelope";
    	LOGGER.entering(CLASS_NAME, methodName, new Object[] {tableName, geometryColumn, sql});
    	
    	delegate.encodeGeometryEnvelope(tableName, geometryColumn, sql);
    	
    	LOGGER.exiting(CLASS_NAME, methodName, sql.toString());
    }
    
    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
    	String methodName = "decodeGeometryEnvelope";
    	LOGGER.entering(CLASS_NAME, methodName, new Object[] {rs, column, cx});
    	LOGGER.exiting(CLASS_NAME, methodName, delegate.decodeGeometryEnvelope(rs, column, cx));
    	return delegate.decodeGeometryEnvelope(rs, column, cx);
    }
    
    @Override
	public FilterToSQL createFilterToSQL() {
    	String methodName = "createFilterToSQL";
    	LOGGER.entering(CLASS_NAME, methodName);
		
		SnowflakeFilterToSQL fts = new SnowflakeFilterToSQL(new StringWriter());
		
		fts.setEscapeBackslash(true);
		
		LOGGER.exiting(CLASS_NAME, methodName, fts);
		return fts;
	}
}
