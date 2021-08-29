package com.orange.marwan.VoiceEventConsumer.objects;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxEventAppliedOffer{
	@JsonProperty("UsageQuantity")
	public String usageQuantity;
	@JsonProperty("UsageQuantityUnit")
	public String usageQuantityUnit;
	@JsonProperty("ProductOfferId")
	public String productOfferId;
	@JsonProperty("ProductOfferExternalId")
	public String productOfferExternalId;
	@JsonProperty("ProductOfferOwnerId")
	public String productOfferOwnerId;
	@JsonProperty("ProductOfferExternalOwnerId")
	public String productOfferExternalOwnerId;
	@JsonProperty("ProductOfferResourceId")
	public String productOfferResourceId;
	@JsonProperty("ProductOfferVersion")
	public String productOfferVersion;
	@JsonProperty("AppliedBundleIndex")
	public String appliedBundleIndex;

	public String getUsageQuantity() {
		return usageQuantity;
	}

	public void setUsageQuantity(String usageQuantity) {
		this.usageQuantity = usageQuantity;
	}

	public String getUsageQuantityUnit() {
		return usageQuantityUnit;
	}

	public void setUsageQuantityUnit(String usageQuantityUnit) {
		this.usageQuantityUnit = usageQuantityUnit;
	}

	public String getProductOfferId() {
		return productOfferId;
	}

	public void setProductOfferId(String productOfferId) {
		this.productOfferId = productOfferId;
	}

	public String getProductOfferExternalId() {
		return productOfferExternalId;
	}

	public void setProductOfferExternalId(String productOfferExternalId) {
		this.productOfferExternalId = productOfferExternalId;
	}

	public String getProductOfferOwnerId() {
		return productOfferOwnerId;
	}

	public void setProductOfferOwnerId(String productOfferOwnerId) {
		this.productOfferOwnerId = productOfferOwnerId;
	}

	public String getProductOfferExternalOwnerId() {
		return productOfferExternalOwnerId;
	}

	public void setProductOfferExternalOwnerId(String productOfferExternalOwnerId) {
		this.productOfferExternalOwnerId = productOfferExternalOwnerId;
	}

	public String getProductOfferResourceId() {
		return productOfferResourceId;
	}

	public void setProductOfferResourceId(String productOfferResourceId) {
		this.productOfferResourceId = productOfferResourceId;
	}

	public String getProductOfferVersion() {
		return productOfferVersion;
	}

	public void setProductOfferVersion(String productOfferVersion) {
		this.productOfferVersion = productOfferVersion;
	}

	public String getAppliedBundleIndex() {
		return appliedBundleIndex;
	}

	public void setAppliedBundleIndex(String appliedBundleIndex) {
		this.appliedBundleIndex = appliedBundleIndex;
	}
}
