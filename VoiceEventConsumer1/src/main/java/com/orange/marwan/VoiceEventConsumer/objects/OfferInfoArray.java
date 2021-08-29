package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OfferInfoArray {
	@JsonProperty("MtxPurchaseEventOfferInfo") 
    public List<MtxPurchaseEventOfferInfo> mtxPurchaseEventOfferInfo;

	public List<MtxPurchaseEventOfferInfo> getMtxPurchaseEventOfferInfo() {
		return mtxPurchaseEventOfferInfo;
	}

	public void setMtxPurchaseEventOfferInfo(List<MtxPurchaseEventOfferInfo> mtxPurchaseEventOfferInfo) {
		this.mtxPurchaseEventOfferInfo = mtxPurchaseEventOfferInfo;
	}
}
