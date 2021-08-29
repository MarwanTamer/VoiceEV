package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxPurchaseEventOfferInfo {
	@JsonProperty("PreActiveState")
	public String preActiveState;
	@JsonProperty("OfferId")
	public String offerId;
	@JsonProperty("OfferVersion")
	public String offerVersion;
	@JsonProperty("ExternalId")
	public String externalId;
	@JsonProperty("ResourceId")
	public String resourceId;

	public String getPreActiveState() {
		return preActiveState;
	}

	public void setPreActiveState(String preActiveState) {
		this.preActiveState = preActiveState;
	}

	public String getOfferId() {
		return offerId;
	}

	public void setOfferId(String offerId) {
		this.offerId = offerId;
	}

	public String getOfferVersion() {
		return offerVersion;
	}

	public void setOfferVersion(String offerVersion) {
		this.offerVersion = offerVersion;
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
}
