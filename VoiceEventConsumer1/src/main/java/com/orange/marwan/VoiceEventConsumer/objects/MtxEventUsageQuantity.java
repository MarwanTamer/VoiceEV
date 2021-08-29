package com.orange.marwan.VoiceEventConsumer.objects;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxEventUsageQuantity {
	@JsonProperty("QuantityType")
	public String quantityType;
	@JsonProperty("QuantityUnit")
	public String quantityUnit;
	@JsonProperty("Flags")
	public String flags;
	@JsonProperty("MsgAmount")
	public String msgAmount;
	@JsonProperty("RatingAmount")
	public String ratingAmount;

	public String getQuantityType() {
		return quantityType;
	}

	public void setQuantityType(String quantityType) {
		this.quantityType = quantityType;
	}

	public String getQuantityUnit() {
		return quantityUnit;
	}

	public void setQuantityUnit(String quantityUnit) {
		this.quantityUnit = quantityUnit;
	}

	public String getFlags() {
		return flags;
	}

	public void setFlags(String flags) {
		this.flags = flags;
	}

	public String getMsgAmount() {
		return msgAmount;
	}

	public void setMsgAmount(String msgAmount) {
		this.msgAmount = msgAmount;
	}

	public String getRatingAmount() {
		return ratingAmount;
	}

	public void setRatingAmount(String ratingAmount) {
		this.ratingAmount = ratingAmount;
	}
}
