package org.geotools.data.snowflake;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.opengis.filter.Filter;

public class SnowflakeSQLDialect extends BasicSQLDialect {

	public SnowflakeSQLDialect(JDBCDataStore dataStore) {
		super(dataStore);
	}

	@Override
	public void initializeConnection(Connection cx) throws SQLException {
		// Implementation of initializing the connection
	}

	@Override
	public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
		if (value == null) {
			sql.append("NULL");
		} else {
			String wkt = value.toText();
			sql.append("ST_GeomFromText('").append(wkt).append("', ").append(srid).append(")");
		}
	}

	@Override
	public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
		sql.append("ST_Envelope(ST_Collect(");
		sql.append(geometryColumn);
		sql.append("))");
	}

	@Override
	public FilterToSQL createFilterToSQL() {
		return new SnowflakeFilterToSQL(new StringWriter());
	}

	// @Override
	public Filter[] splitFilter(Filter filter, SimpleFeatureType schema) {
		return new Filter[] { filter, null };
	}

	@Override
	public String[] getDesiredTablesType() {
		return new String[] { "TABLE" };
	}

	@Override
	public void encodeValue(Object value, Class type, StringBuffer sql) {
		if (value == null) {
			sql.append("NULL");
		} else if (value instanceof Geometry) {
			try {
				encodeGeometryValue((Geometry) value, 2, -1, sql);
			} catch (IOException e) {
				throw new RuntimeException("Error encoding geometry", e);
			}
		} else {
			super.encodeValue(value, type, sql);
		}
	}

	@Override
	public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		// Implementation of getting geometry SRID
		return null;
	}

	@Override
	public int getGeometryDimension(String schemaName, String tableName, String columnName, Connection cx)
			throws SQLException {
		// Implementation of getting geometry dimension
		return 2;
	}

	@Override
	public void handleUserDefinedType(ResultSet columnMetaData, ColumnMetadata metadata, Connection cx)
			throws SQLException {
		// Implementation of handling user-defined type
	}

	@Override
	public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

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
			String msg = "Error decoding wkb";
			throw (IOException) new IOException(msg).initCause(e);
		}

	}
}
