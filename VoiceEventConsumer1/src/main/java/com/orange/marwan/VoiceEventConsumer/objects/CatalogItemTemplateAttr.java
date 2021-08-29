package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogItemTemplateAttr {
	@JsonProperty("OEGCatalogItems")
	public OEGCatalogItems oEGCatalogItems;

	public OEGCatalogItems getoEGCatalogItems() {
		return oEGCatalogItems;
	}

	public void setoEGCatalogItems(OEGCatalogItems oEGCatalogItems) {
		this.oEGCatalogItems = oEGCatalogItems;
	}
}
