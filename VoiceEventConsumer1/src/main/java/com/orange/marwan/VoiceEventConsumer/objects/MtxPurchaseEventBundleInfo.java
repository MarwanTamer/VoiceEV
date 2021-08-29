package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxPurchaseEventBundleInfo {
	@JsonProperty("PreActiveState") 
    public String preActiveState;
    @JsonProperty("CatalogItemId") 
    public String catalogItemId;
    @JsonProperty("CatalogItemExternalId") 
    public String catalogItemExternalId;
    @JsonProperty("CatalogItemTemplateAttr") 
    public CatalogItemTemplateAttr catalogItemTemplateAttr;
    @JsonProperty("BundleId") 
    public String bundleId;
    @JsonProperty("BundleVersion") 
    public String bundleVersion;
    @JsonProperty("ExternalId") 
    public String externalId;
    @JsonProperty("ResourceId") 
    public String resourceId;
    @JsonProperty("PurchasedBundleAttr") 
	public PurchasedBundleAttr purchasedBundleAttr;

	public String getPreActiveState() {
		return preActiveState;
	}

	public void setPreActiveState(String preActiveState) {
		this.preActiveState = preActiveState;
	}

	public String getCatalogItemId() {
		return catalogItemId;
	}

	public void setCatalogItemId(String catalogItemId) {
		this.catalogItemId = catalogItemId;
	}

	public String getCatalogItemExternalId() {
		return catalogItemExternalId;
	}

	public void setCatalogItemExternalId(String catalogItemExternalId) {
		this.catalogItemExternalId = catalogItemExternalId;
	}

	public CatalogItemTemplateAttr getCatalogItemTemplateAttr() {
		return catalogItemTemplateAttr;
	}

	public void setCatalogItemTemplateAttr(CatalogItemTemplateAttr catalogItemTemplateAttr) {
		this.catalogItemTemplateAttr = catalogItemTemplateAttr;
	}

	public String getBundleId() {
		return bundleId;
	}

	public void setBundleId(String bundleId) {
		this.bundleId = bundleId;
	}

	public String getBundleVersion() {
		return bundleVersion;
	}

	public void setBundleVersion(String bundleVersion) {
		this.bundleVersion = bundleVersion;
	}

	public String getExternalId() {
		return externalId;
	}

	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public PurchasedBundleAttr getPurchasedBundleAttr() {
		return purchasedBundleAttr;
	}

	public void setPurchasedBundleAttr(PurchasedBundleAttr purchasedBundleAttr) {
		this.purchasedBundleAttr = purchasedBundleAttr;
	}
}
