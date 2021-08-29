package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxEventCollection {
	@JsonProperty("EventList")
	EventList EventList;

	public EventList getEventList() {
		return EventList;
	}

	public void setEventList(EventList eventList) {
		EventList = eventList;
	}
}
