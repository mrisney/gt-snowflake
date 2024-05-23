package com.comsonics.snowflake;

import java.io.IOException;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.FeatureVisitor;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;

public class SnowflakeFeatureSource extends ContentFeatureSource {

	public SnowflakeFeatureSource(ContentEntry entry, Query query) {
        super(entry, query);
    }
	
	/** Access parent CSVDataStore. */
    public SnowflakeDataStore getDataStore() {
        return (SnowflakeDataStore) super.getDataStore();
    }
    
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new SnowflakeFeatureReader(getState(), query);
    }
    
    protected int getCountInternal(Query query) throws IOException {
        /*if (query.getFilter() == Filter.INCLUDE) {
            SnowflakeReader reader = getDataStore().read();
            try {
                boolean connect = reader.readHeaders();
                if (connect == false) {
                    throw new IOException("Unable to connect");
                }
                int count = 0;
                while (reader.readRecord()) {
                    count += 1;
                }
                return count;
            } finally {
                reader.close();
            }
        }*/
        return -1; // feature by feature scan required to count records
    }
    
    /**
     * Implementation that generates the total bounds (many file formats record this information in
     * the header)
     */
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return null; // feature by feature scan required to establish bounds
    }
    
    protected SimpleFeatureType buildFeatureType() throws IOException {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(entry.getName());
        

        // read headers
        /*SnowflakeReader reader = getDataStore().read();
        try {
            boolean success = reader.readHeaders();
            if (success == false) {
                throw new IOException("Header of CSV file not available");
            }*/

            // we are going to hard code a Geometry object
            // The PointsLatLong column will be put into a Geometry called SnowShape
            
            //builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system TODO: Change this to 4326
            builder.setSRS( "EPSG:4326" );
            builder.add("SnowShape", Geometry.class);

            //Loop through the file and add the other columns as features?
            /*for (String column : reader.getHeaders()) {
                if ("lat".equalsIgnoreCase(column)) {
                    continue; // skip as it is part of Location
                }
                if ("lon".equalsIgnoreCase(column)) {
                    continue; // skip as it is part of Location
                }
                builder.add(column, String.class);
            }
            */

            // build the type (it is immutable and cannot be modified)
            final SimpleFeatureType SCHEMA = builder.buildFeatureType();
            return SCHEMA;
        /*} finally {
            reader.close();
        }*/
    }
    
    /**
     * Make handleVisitor package visible allowing CSVFeatureStore to delegate to this
     * implementation.
     */
    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException {
        return super.handleVisitor(query, visitor);
        // WARNING: Please note this method is in SnowflakeFeatureSource!
    }
}
