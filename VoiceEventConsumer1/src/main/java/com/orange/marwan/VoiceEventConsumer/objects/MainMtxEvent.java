package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MainMtxEvent {
	@JsonProperty("MtxEventCollection")
	MtxEventCollection MtxEventCollection;

	public MtxEventCollection getMtxEventCollection() {
		return MtxEventCollection;
	}

	public void setMtxEventCollection(MtxEventCollection mtxEventCollection) {
		MtxEventCollection = mtxEventCollection;
	}
}
