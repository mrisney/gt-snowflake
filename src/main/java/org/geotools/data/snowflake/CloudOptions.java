package org.geotools.data.snowflake;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudOptions {
	
	private static final Map<String, String> AWS_REGIONS = new HashMap<>();
    private static final Map<String, String> GCP_REGIONS = new HashMap<>();
    private static final Map<String, String> AZURE_REGIONS = new HashMap<>();
    private static final Map<String, String> CLOUD_PROVIDERS = new HashMap<>();

    // Cloud Providers
    static {
        CLOUD_PROVIDERS.put("Amazon Web Services (AWS)", "AWS");
        CLOUD_PROVIDERS.put("Google Cloud Platform (GCP)", "GCP");
        CLOUD_PROVIDERS.put("Microsoft Azure", "Azure");
    }

    // AWS Regions
    static {
        // North + South America Regions
        AWS_REGIONS.put("US West (Oregon)", "us-west-2");
        AWS_REGIONS.put("US East (Ohio)", "us-east-2");
        AWS_REGIONS.put("US East (N. Virginia)", "us-east-1");
        AWS_REGIONS.put("Canada (Central)", "ca-central-1");
        AWS_REGIONS.put("South America (Sao Paulo)", "sa-east-1");
        AWS_REGIONS.put("US East (Commercial Gov - N. Virginia)", "us-east-1");
        AWS_REGIONS.put("US Gov West 1", "us-gov-west-1");
        AWS_REGIONS.put("US Gov 1 West", "us-gov-1-west");
        AWS_REGIONS.put("US Gov East 1", "us-gov-east-1");

        // Europe + Middle East Regions
        AWS_REGIONS.put("EU (Ireland)", "eu-west-1");
        AWS_REGIONS.put("Europe (London)", "eu-west-2");
        AWS_REGIONS.put("EU (Paris)", "eu-west-3");
        AWS_REGIONS.put("EU (Frankfurt)", "eu-central-1");
        AWS_REGIONS.put("EU (Stockholm)", "eu-north-1");

        // Asia Pacific Regions
        AWS_REGIONS.put("Asia Pacific (Tokyo)", "ap-northeast-1");
        AWS_REGIONS.put("Asia Pacific (Osaka)", "ap-northeast-3");
        AWS_REGIONS.put("Asia Pacific (Seoul)", "ap-northeast-2");
        AWS_REGIONS.put("Asia Pacific (Mumbai)", "ap-south-1");
        AWS_REGIONS.put("Asia Pacific (Singapore)", "ap-southeast-1");
        AWS_REGIONS.put("Asia Pacific (Sydney)", "ap-southeast-2");
        AWS_REGIONS.put("Asia Pacific (Jakarta)", "ap-southeast-3");
    }

    // GCP Regions
    static {
        // North + South America Regions
        GCP_REGIONS.put("US Central1 (Iowa)", "us-central1");
        GCP_REGIONS.put("US East4 (N. Virginia", "us-east4");

        // Europe + Middle East Regions
        GCP_REGIONS.put("Europe West2 (London)", "europe-west2");
        GCP_REGIONS.put("Europe West4 (Netherlands)", "europe-west4");

        // Asia Pacific Regions
    }

    // Azure Regions
    static {
        // North + South America Regions
        AZURE_REGIONS.put("West US 2 (Washington)", "west-us-2");
        AZURE_REGIONS.put("Central US (Iowa)", "central-us");
        AZURE_REGIONS.put("South Central US (Texas)", "south-central-us");
        AZURE_REGIONS.put("East US 2 (Virginia)", "east-us-2");
        AZURE_REGIONS.put("Canada Central (Toronto)", "canada-central");
        AZURE_REGIONS.put("South Central US (Texas)", "SOUTH-CENTRAL-US");
        AZURE_REGIONS.put("US Gov Virginia", "us-gov-virginia");

        // Europe + Middle East Regions
        AZURE_REGIONS.put("UK South (London)", "uk-south");
        AZURE_REGIONS.put("North Europe (Ireland)", "north-europe");
        AZURE_REGIONS.put("West Europe (Netherlands)", "west-europe");
        AZURE_REGIONS.put("Switzerland North (Zurich)", "switzerland-north");
        AZURE_REGIONS.put("UAE North (Dubai)", "uae-north");

        // Asia Pacific Regions
        AZURE_REGIONS.put("Central India (Pune)", "central-india");
        AZURE_REGIONS.put("Japan East (Tokyo)", "japan-east");
        AZURE_REGIONS.put("Southeast Asia (Singapore)", "southeast-asia");
        AZURE_REGIONS.put("Australia East (New South Wales)", "australia-east");
    }
    
    public static List<String> getCloudOptions() {
    	List<String> cloudOptions = new ArrayList<String>();
    	
    	for (Map.Entry<String, ?> entry : AWS_REGIONS.entrySet()) {
    		cloudOptions.add("AWS : " + entry.getValue());
    	}
    	
    	for (Map.Entry<String, ?> entry : AZURE_REGIONS.entrySet()) {
    		cloudOptions.add("Azure : " + entry.getValue());
    	}
    	
    	for (Map.Entry<String, ?> entry : GCP_REGIONS.entrySet()) {
    		cloudOptions.add("GCP : " + entry.getValue());
    	}
    	
    	return cloudOptions;
    }

}
