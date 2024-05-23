package com.comsonics.snowflake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.locationtech.jts.geom.Point;

public class SnowflakeDataStore extends ContentDataStore {
	
	String username;
	String password;
	String account;
	String db;
	String schema;
	
	
	public SnowflakeDataStore(String username, String password, String account, String db, String schema) {
		this.username = username;
		this.password = password;
		this.account = account;
		this.db = db;
		this.schema = schema;
	}
	
	//TODO: Figure out what this actually does
	protected List<Name> createTypeNames() throws IOException {
        
		//String name = file.getName();
        //name = name.substring(0, name.lastIndexOf('.'));

        Name typeName = new NameImpl("locations");
        return Collections.singletonList(typeName);
    }
	
	@Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new SnowflakeFeatureSource(entry, Query.ALL);
    }
	
	@Override
    public void createSchema(SimpleFeatureType featureType) throws IOException {
        List<String> header = new ArrayList<>();
        GeometryDescriptor geometryDescrptor = featureType.getGeometryDescriptor();
        if (geometryDescrptor != null
        		//TODO: Check for EPSG:4326 CRS
                /*&& CRS.equalsIgnoreMetadata(
                        DefaultGeographicCRS.WGS84,
                        geometryDescrptor.getCoordinateReferenceSystem())*/
                && geometryDescrptor.getType().getBinding().isAssignableFrom(Point.class)) {
            header.add("LAT");
            header.add("LON");
        } else {
            throw new IOException("Unable use LAT/LON to represent " + geometryDescrptor);
        }
        for (AttributeDescriptor descriptor : featureType.getAttributeDescriptors()) {
            if (descriptor instanceof GeometryDescriptor) continue;
            header.add(descriptor.getLocalName());
        }
        // Write out header, producing an empty file of the correct type
        /*SnowflakeWriter writer = new SnowflakeWriter(new FileWriter(file), ',');
        try {
            writer.writeRecord(header.toArray(new String[header.size()]));
        } finally {
            writer.close();
        }*/
    }

}
