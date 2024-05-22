package com.comsonics.snowflake.ui;

import org.apache.wicket.markup.html.basic.Label;
import org.geoserver.web.GeoServerBasePage;
public class ConnectionPage extends GeoServerBasePage {

	public ConnectionPage() {
		add(new Label("connectionlabel", "Connection Details"));
	}

}
