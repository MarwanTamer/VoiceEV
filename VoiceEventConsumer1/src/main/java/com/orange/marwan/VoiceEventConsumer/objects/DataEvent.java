package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEvent {
	ObjectMapper mapper;
	@JsonProperty("OEGDataEvent")
	public OEGEvent OEGDataEvent;

	public OEGEvent getOEGDataEvent() {
		return OEGDataEvent;
	}

	public void setOEGDataEvent(OEGEvent oEGDataEvent) {
		OEGDataEvent = oEGDataEvent;
	}
}
