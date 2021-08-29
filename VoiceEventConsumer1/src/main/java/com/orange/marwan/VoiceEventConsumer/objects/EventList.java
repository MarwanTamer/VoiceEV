package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventList {
	@JsonProperty("OEGDataEvent")
	ArrayList<OEGEvent> OEGDataEvent;
	@JsonProperty("OEGVoiceEvent")
	ArrayList<OEGEvent> OEGVoiceEvent;
	@JsonProperty("OEGTextEvent")
	ArrayList<OEGEvent> OEGTextEvent;

	public ArrayList<OEGEvent> getOEGDataEvent() {
		return OEGDataEvent;
	}

	public void setOEGDataEvent(ArrayList<OEGEvent> oEGDataEvent) {
		OEGDataEvent = oEGDataEvent;
	}

	public ArrayList<OEGEvent> getOEGVoiceEvent() {
		return OEGVoiceEvent;
	}

	public void setOEGVoiceEvent(ArrayList<OEGEvent> oEGVoiceEvent) {
		OEGVoiceEvent = oEGVoiceEvent;
	}

	public ArrayList<OEGEvent> getOEGTextEvent() {
		return OEGTextEvent;
	}

	public void setOEGTextEvent(ArrayList<OEGEvent> oEGTextEvent) {
		OEGTextEvent = oEGTextEvent;
	}
}
