package com.comsonics.snowflake.ui;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.wicket.markup.html.form.IChoiceRenderer;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;

@SuppressWarnings("serial")
public class SnowflakeParamPanel extends Panel {

    String username;
    String password;
    String accountIdentifier;
    // String region;
    // String cloudProvider;
    String databaseName;
    String schemaName;
    String connectionURL;

    // CloudProviderModel cloudProviderModel = new CloudProviderModel();
    // RegionModel regionModel = new RegionModel();

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
        AZURE_REGIONS.put("West US 2 (Washington)", "westus2");
        AZURE_REGIONS.put("Central US (Iowa)", "centralus");
        AZURE_REGIONS.put("South Central US (Texas)", "southcentralus");
        AZURE_REGIONS.put("East US 2 (Virginia)", "eastus2");
        AZURE_REGIONS.put("Canada Central (Toronto)", "canadacentral");
        AZURE_REGIONS.put("South Central US (Texas)", "SOUTHCENTRALUS");
        AZURE_REGIONS.put("US Gov Virginia", "usgovvirginia");

        // Europe + Middle East Regions
        AZURE_REGIONS.put("UK South (London)", "uksouth");
        AZURE_REGIONS.put("North Europe (Ireland)", "northeurope");
        AZURE_REGIONS.put("West Europe (Netherlands)", "westeurope");
        AZURE_REGIONS.put("Switzerland North (Zurich)", "switzerlandnorth");
        AZURE_REGIONS.put("UAE North (Dubai)", "uaenorth");

        // Asia Pacific Regions
        AZURE_REGIONS.put("Central India (Pune)", "centralindia");
        AZURE_REGIONS.put("Japan East (Tokyo)", "japaneast");
        AZURE_REGIONS.put("Southeast Asia (Singapore)", "southeastasia");
        AZURE_REGIONS.put("Australia East (New South Wales)", "australiaeast");
    }

    public SnowflakeParamPanel(String id
            // CloudProviderModel cloudProviderModel, RegionModel regionModel
            ) {
        this(
                id,
                // cloudProviderModel, regionModel,
                null,
                null,
                null,
                null,
                // null,
                // null,
                null);
    }

    public SnowflakeParamPanel(
            String id,
            // CloudProviderModel cloudProviderModel,
            // RegionModel regionModel,
            String username,
            String password,
            String accountIdentifier
            // String region,
            // String cloudProvider
            ) {
        this(
                id,
                // cloudProviderModel,
                // regionModel,
                username,
                password,
                accountIdentifier,
                // region,
                // cloudProvider,
                null,
                null);
    }

    public SnowflakeParamPanel(
            String id,
            // CloudProviderModel cloudProviderModel,
            // RegionModel regionModel,
            String username,
            String password,
            String accountIdentifier,
            // String region,
            // String cloudProvider,
            String databaseName) {
        this(
                id,
                // cloudProviderModel,
                // regionModel,
                username,
                password,
                accountIdentifier,
                // region,
                // cloudProvider,
                databaseName,
                null);
    }

    public SnowflakeParamPanel(
            String id,
            // CloudProviderModel cloudProviderModel,
            // RegionModel regionModel,
            String username,
            String password,
            String accountIdentifier,
            // String region,
            // String cloudProvider,
            String databaseName,
            String schemaName) {
        super(id);

        this.username = username;
        this.password = password;
        this.accountIdentifier = accountIdentifier;
        // this.region = region;
        // this.cloudProvider = cloudProvider;
        this.databaseName = databaseName;
        this.schemaName = schemaName;

        // this.cloudProviderModel = cloudProviderModel;
        // this.regionModel = regionModel;

        this.connectionURL =
                "jdbc:snowflake://"
                        + accountIdentifier
                        // + "."
                        // + region
                        // + "."
                        // + cloudProvider
                        + ".snowflakecomputing.com";

        add(new TextField<>("username", new PropertyModel<>(this, "username")).setRequired(true));
        add(
                new PasswordTextField("password", new PropertyModel<>(this, "password"))
                        .setResetPassword(false)
                        .setRequired(true));
        add(
                new TextField<>("accountIdentifier", new PropertyModel<>(this, "accountIdentifier"))
                        .setRequired(true));

        // DropDownChoice<SelectOption> cloudProviderChoice = createCloudProviderDropDown();
        // cloudProviderChoice.setModelObject(new SelectOption("Microsoft Azure", "Azure"));
        // add(cloudProviderChoice);

        // DropDownChoice<SelectOption> regionChoice = createRegionDropDown();
        // regionChoice.setModelObject(new SelectOption("West US 2 (Washington)", "westus2"));
        // add(regionChoice);

        add(
                new TextField<>("databaseName", new PropertyModel<>(this, "databaseName"))
                        .setRequired(false));
        add(
                new TextField<>("schemaName", new PropertyModel<>(this, "schemaName"))
                        .setRequired(false));
    }

    public ArrayList<SelectOption> getCloudProvidersList() {
        Map<String, String> cloudProvidersTemp = new HashMap<String, String>();

        cloudProvidersTemp.put("AWS", "Amazon Web Services (AWS)");
        cloudProvidersTemp.put("GCP", "Google Cloud Platform (GCP)");
        cloudProvidersTemp.put("Azure", "Microsoft Azure");

        ArrayList<SelectOption> list = new ArrayList<SelectOption>();
        for (Map.Entry<String, String> entry : cloudProvidersTemp.entrySet()) {
            list.add(new SelectOption(entry.getKey(), entry.getValue()));
        }

        return list;
    }

    public ArrayList<SelectOption> getAzureRegionsList() {
        Map<String, String> azureRegionsTemp = new HashMap<String, String>();

        // North + South America Regions
        azureRegionsTemp.put("westus2", "West US 2 (Washington)");
        azureRegionsTemp.put("centralus", "Central US (Iowa)");
        azureRegionsTemp.put("southcentralus", "South Central US (Texas)");
        azureRegionsTemp.put("eastus2", "East US 2 (Virginia)");
        azureRegionsTemp.put("canadacentral", "Canada Central (Toronto)");
        azureRegionsTemp.put("southcentralus", "South Central US (Texas)");
        azureRegionsTemp.put("usgovvirginia", "US Gov Virginia");

        // Europe + Middle East Regions
        azureRegionsTemp.put("uksouth", "UK South (London)");
        azureRegionsTemp.put("northeurope", "North Europe (Ireland)");
        azureRegionsTemp.put("westeurope", "West Europe (Netherlands)");
        azureRegionsTemp.put("switzerlandnorth", "Switzerland North (Zurich)");
        azureRegionsTemp.put("uaenorth", "UAE North (Dubai)");

        // Asia Pacific Regions
        azureRegionsTemp.put("centralindia", "Central India (Pune)");
        azureRegionsTemp.put("japaneast", "Japan East (Tokyo)");
        azureRegionsTemp.put("southeastasia", "Southeast Asia (Singapore)");
        azureRegionsTemp.put("australiaeast", "Australia East (New South Wales)");

        ArrayList<SelectOption> list = new ArrayList<SelectOption>();
        for (Map.Entry<String, String> entry : azureRegionsTemp.entrySet()) {
            list.add(new SelectOption(entry.getKey(), entry.getValue()));
        }

        return list;
    }

    public ArrayList<SelectOption> getDisplayListForDropdown(Map<String, String> map) {
        ArrayList<SelectOption> list = new ArrayList<SelectOption>();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            list.add(new SelectOption(entry.getKey(), entry.getValue()));
        }

        return list;
    }
    /*
    private DropDownChoice<SelectOption> createCloudProviderDropDown() {

        DropDownChoice<SelectOption> cloudProviderChoice =
                new DropDownChoice<SelectOption>(
                        "cloudProvider",
                        new PropertyModel<SelectOption>(cloudProviderModel, "cloudProvider"),
                        getCloudProvidersList(),
                        new ChoiceRenderer<SelectOption>("value", "key"));
        /*new CustomChoiceRenderer()) {

            private static final long serialVersionUID = 1L;

            @Override
            protected boolean wantOnSelectionChangedNotifications() {

                return true;
            }

            @Override
            protected void onSelectionChanged(SelectOption newSelection) {
                cloudProvider = ((SelectOption) newSelection).getKey();
                super.onSelectionChanged(newSelection);
            }
        };
        cloudProviderChoice.setRequired(true);
        return cloudProviderChoice;
    }

    private DropDownChoice<SelectOption> createRegionDropDown() {

        DropDownChoice<SelectOption> regionChoice =
                new DropDownChoice<SelectOption>(
                        "region",
                        new PropertyModel<SelectOption>(regionModel, "region"),
                        getAzureRegionsList(),
                        new ChoiceRenderer<SelectOption>("value", "key"));
        /*new CustomChoiceRenderer()) {

            private static final long serialVersionUID = 1L;

            @Override
            protected boolean wantOnSelectionChangedNotifications() {

                return true;
            }

            @Override
            protected void onSelectionChanged(SelectOption newSelection) {
                region = ((SelectOption) newSelection).getKey();
                super.onSelectionChanged(newSelection);
            }
        };
        regionChoice.setRequired(true);
        return regionChoice;
    }
    */

    class CustomChoiceRenderer implements IChoiceRenderer<SelectOption> {

        private static final long serialVersionUID = 1L;

        @Override
        public Object getDisplayValue(SelectOption object) {

            return object.getValue() + "-" + object.getKey();
        }

        @Override
        public String getIdValue(SelectOption object, int index) {

            return object.getKey();
        }

        @Override
        public SelectOption getObject(
                String id, IModel<? extends List<? extends SelectOption>> choices) {

            return null;
        }
    }
}
