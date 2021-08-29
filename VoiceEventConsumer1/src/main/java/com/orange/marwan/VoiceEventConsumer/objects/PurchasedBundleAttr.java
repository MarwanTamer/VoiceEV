package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PurchasedBundleAttr {
	@JsonProperty("OEGPurchasedOfferExtension") 
    public OEGPurchasedOfferExtension oEGPurchasedOfferExtension;

	public OEGPurchasedOfferExtension getoEGPurchasedOfferExtension() {
		return oEGPurchasedOfferExtension;
	}

	public void setoEGPurchasedOfferExtension(OEGPurchasedOfferExtension oEGPurchasedOfferExtension) {
		this.oEGPurchasedOfferExtension = oEGPurchasedOfferExtension;
	}
}
