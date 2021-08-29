package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OEGPurchasedOfferExtension {
	@JsonProperty("TargetMSISDN") 
    public String targetMSISDN;
    @JsonProperty("ServiceAction") 
    public String serviceAction;
    @JsonProperty("ListName") 
    public String listName;
	public String getTargetMSISDN() {
		return targetMSISDN;
	}
	public void setTargetMSISDN(String targetMSISDN) {
		this.targetMSISDN = targetMSISDN;
	}
	public String getServiceAction() {
		return serviceAction;
	}
	public void setServiceAction(String serviceAction) {
		this.serviceAction = serviceAction;
	}
	public String getListName() {
		return listName;
	}
	public void setListName(String listName) {
		this.listName = listName;
	}
}
