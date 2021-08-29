package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AppliedOfferArray {
	@JsonProperty("MtxEventAppliedOffer")
	public List<MtxEventAppliedOffer> mtxEventAppliedOffer;

	public List<MtxEventAppliedOffer> getMtxEventAppliedOffer() {
		return mtxEventAppliedOffer;
	}

	public void setMtxEventAppliedOffer(List<MtxEventAppliedOffer> mtxEventAppliedOffer) {
		this.mtxEventAppliedOffer = mtxEventAppliedOffer;
	}
}
