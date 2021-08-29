package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MeterUpdateArray {
	@JsonProperty("MtxMeterUpdate")
	public List<MtxMeterUpdate> mtxMeterUpdate;

	public List<MtxMeterUpdate> getMtxMeterUpdate() {
		return mtxMeterUpdate;
	}

	public void setMtxMeterUpdate(List<MtxMeterUpdate> mtxMeterUpdate) {
		this.mtxMeterUpdate = mtxMeterUpdate;
	}
}
