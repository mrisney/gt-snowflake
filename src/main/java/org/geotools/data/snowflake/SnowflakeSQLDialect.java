package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.util.factory.Hints;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.opengis.filter.Filter;

public class SnowflakeSQLDialect extends BasicSQLDialect {
	
	private static final Logger LOGGER = Logging.getLogger(SnowflakeSQLDialect.class);

	public SnowflakeSQLDialect(JDBCDataStore dataStore) {
		super(dataStore);
		LOGGER.log(Level.INFO, "Initializing SnowflakeSQLDialect()");
	}

	@Override
	public void initializeConnection(Connection cx) throws SQLException {
		LOGGER.log(Level.INFO, "initializeConnection() -- Start");
		// Implementation of initializing the connection
		LOGGER.log(Level.INFO, "initializeConnection() -- End");
	}

	@Override
	public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
		LOGGER.log(Level.INFO, "encodeGeometryValue() -- Start");
		
		if (value == null) {
			LOGGER.log(Level.INFO, "Geometry value is NULL");
			sql.append("NULL");
			LOGGER.log(Level.INFO, "encodeGeometryValue() -- \n\tGeometry Value: NULL\n\tDimension: " + dimension + "\n\tSRID: " + srid + "\n\tSQL: " + sql.toString());
		} else {
			String wkt = value.toText();
			LOGGER.log(Level.INFO, "Geometry WKT -- " + wkt);
			sql.append("ST_GeomFromText('").append(wkt).append("', ").append(srid).append(")");
			LOGGER.log(Level.INFO, "encodeGeometryValue() -- \n\tGeometry Value: " + value.toText() + "\n\tDimension: " + dimension + "\n\tSRID: " + srid + "\n\tSQL: " + sql.toString());
		}
		
		
		LOGGER.log(Level.INFO, "encodeGeometryValue() -- End");
	}

	@Override
	public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
		LOGGER.log(Level.INFO, "encodeGeometryEnvelope() -- Start");
		
		sql.append("ST_Envelope(ST_Collect(");
		sql.append(geometryColumn);
		sql.append("))");
		
		LOGGER.log(Level.INFO, "encodeGeometryEnvelope() -- Parameters:\n\tTableName: " + tableName + "\n\tGeometryColumn: " + geometryColumn + "\n\tSQL: " + sql.toString());
		
		LOGGER.log(Level.INFO, "encodeGeometryEnvelope() -- End");
	}

	@Override
	public FilterToSQL createFilterToSQL() {
		LOGGER.log(Level.INFO, "createFilterToSQL()");
		return new SnowflakeFilterToSQL(new StringWriter());
	}

	// @Override
	public Filter[] splitFilter(Filter filter, SimpleFeatureType schema) {
		LOGGER.log(Level.INFO, "splitFilter()");
		return new Filter[] { filter, null };
	}

	@Override
	public String[] getDesiredTablesType() {
		LOGGER.log(Level.INFO, "getDesiredTablesType() -- [TABLE]");
		return new String[] { "TABLE" };
	}

	@Override
	public void encodeValue(Object value, Class type, StringBuffer sql) {
		LOGGER.log(Level.INFO, "encodeValue() -- Start");
		
		if (value == null) {
			LOGGER.log(Level.INFO, "Value is NULL");
			sql.append("NULL");
			LOGGER.log(Level.INFO, "encodeValue() -- Parameters:\n\tValue: NULL\n\tClass: " + type.getName() + "\n\tSQL: " + sql.toString());
		} else if (value instanceof Geometry) {
			try {
				LOGGER.log(Level.INFO, "Encoding Geometry value");
				encodeGeometryValue((Geometry) value, 2, -1, sql);
				LOGGER.log(Level.INFO, "encodeValue() -- Parameters:\n\tValue: " + value.toString() + "\n\tClass: " + type.getName() + "\n\tSQL: " + sql.toString());
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, "Error encoding geometry");
				throw new RuntimeException("Error encoding geometry", e);
			}
		} else {
			LOGGER.log(Level.INFO, "Value is not an instance of Geometry");
			super.encodeValue(value, type, sql);
			LOGGER.log(Level.INFO, "encodeValue() -- Parameters:\n\tValue: " + value.toString() + "\n\tClass: " + type.getName() + "\n\tSQL: " + sql.toString());
		}
		
		LOGGER.log(Level.INFO, "encodeValue() -- End");
	}

	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		// Implementation of getting geometry SRID
		LOGGER.log(Level.INFO, "getGeometrySRID() -- Start");
		LOGGER.log(Level.INFO, "getGeometrySRID() -- Parameters:\n\tSchema: " + schemaName + "\n\tTable: " + tableName + "\n\tColumn: " + columnName);
		LOGGER.log(Level.WARNING, "getGeometrySRID() -- Hard-coded to return null");
		LOGGER.log(Level.INFO, "getGeometrySRID() -- End");
		return null;
	}

	@Override
	public int getGeometryDimension(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		// Implementation of getting geometry dimension
		LOGGER.log(Level.INFO, "getGeometryDimension() -- Start");
		LOGGER.log(Level.INFO, "getGeometryDimension() -- Parameters:\n\tSchema: " + schemaName + "\n\tTable: " + tableName + "\n\tColumn: " + columnName);
		LOGGER.log(Level.WARNING, "getGeometryDimension() -- Hard-coded to return 2");
		LOGGER.log(Level.INFO, "getGeometryDimension() -- End");
		return 2;
	}

	@Override
	public void handleUserDefinedType(ResultSet columnMetaData, ColumnMetadata metadata, Connection cx)
			throws SQLException {
		LOGGER.log(Level.INFO, "handleUserDefinedType() -- Start");
		LOGGER.log(Level.WARNING, "handleUserDefinedType() -- Not yet implemented");
		LOGGER.log(Level.INFO, "handleUserDefinedType() -- Start");
		// Implementation of handling user-defined type
	}

	@Override
	public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
		LOGGER.log(Level.INFO, "decodeGeometryEnvelope() -- Start");
		LOGGER.log(Level.WARNING, "decodeGeometryEnvelope() -- Hard-coded to return null");
		LOGGER.log(Level.INFO, "decodeGeometryEnvelope() -- Start");
		return null;
	}

	@Override
	public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column,
			GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
		LOGGER.log(Level.INFO, "decodeGeometryValue() -- Start");
		byte[] bytes = rs.getBytes(column);
		if (bytes == null) {
			LOGGER.log(Level.INFO, "ResultSet bytes are NULL -- returning null");
			return null;
		}

		try {
			LOGGER.log(Level.INFO, "decodeGeometryValue() -- End");
			return new WKBReader(factory).read(bytes);
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Error decoding WKB");
			LOGGER.log(Level.INFO, "decodeGeometryValue() -- End");
			String msg = "Error decoding wkb";
			throw (IOException) new IOException(msg).initCause(e);
		}
		
		

	}
}
