package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxBalanceUpdate {
	@JsonProperty("BalanceClassId")
	public String balanceClassId;
	@JsonProperty("BalanceTemplateId")
	public String balanceTemplateId;
	@JsonProperty("BalanceResourceId")
	public String balanceResourceId;
	@JsonProperty("BalanceStartTime")
	public String balanceStartTime;
	@JsonProperty("BalanceEndTime")
	public String balanceEndTime;
	@JsonProperty("Flags")
	public String flags;
	@JsonProperty("Amount")
	public String amount;

	public String getBalanceClassId() {
		return balanceClassId;
	}

	public void setBalanceClassId(String balanceClassId) {
		this.balanceClassId = balanceClassId;
	}

	public String getBalanceTemplateId() {
		return balanceTemplateId;
	}

	public void setBalanceTemplateId(String balanceTemplateId) {
		this.balanceTemplateId = balanceTemplateId;
	}

	public String getBalanceResourceId() {
		return balanceResourceId;
	}

	public void setBalanceResourceId(String balanceResourceId) {
		this.balanceResourceId = balanceResourceId;
	}

	public String getBalanceStartTime() {
		return balanceStartTime;
	}

	public void setBalanceStartTime(String balanceStartTime) {
		this.balanceStartTime = balanceStartTime;
	}

	public String getBalanceEndTime() {
		return balanceEndTime;
	}

	public void setBalanceEndTime(String balanceEndTime) {
		this.balanceEndTime = balanceEndTime;
	}

	public String getFlags() {
		return flags;
	}

	public void setFlags(String flags) {
		this.flags = flags;
	}

	public String getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		this.amount = amount;
	}
}
