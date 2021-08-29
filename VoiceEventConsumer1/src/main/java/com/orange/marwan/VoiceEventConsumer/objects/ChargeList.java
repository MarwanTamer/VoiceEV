package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChargeList {
	@JsonProperty("MtxEventCharge")
	public List<MtxEventCharge> mtxEventCharge;

	public List<MtxEventCharge> getMtxEventCharge() {
		return mtxEventCharge;
	}

	public void setMtxEventCharge(List<MtxEventCharge> mtxEventCharge) {
		this.mtxEventCharge = mtxEventCharge;
	}
}
