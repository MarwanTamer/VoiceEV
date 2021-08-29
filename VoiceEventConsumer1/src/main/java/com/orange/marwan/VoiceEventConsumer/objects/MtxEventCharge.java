package com.orange.marwan.VoiceEventConsumer.objects;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MtxEventCharge {
	@JsonProperty("UsageQuantity")
	public String usageQuantity;
	@JsonProperty("UsageQuantityUnit")
	public String usageQuantityUnit;
	@JsonProperty("AppliedOfferIndex")
	public String appliedOfferIndex;
	@JsonProperty("BalanceUpdateIndex")
	public String balanceUpdateIndex;
	@JsonProperty("UpdateType")
	public String updateType;
	@JsonProperty("Amount")
	public String amount;
	@JsonProperty("ImpactSource")
	public String impactSource;
	@JsonProperty("PaymentType")
	public String paymentType;

	public String getUsageQuantity() {
		return usageQuantity;
	}

	public void setUsageQuantity(String usageQuantity) {
		this.usageQuantity = usageQuantity;
	}

	public String getUsageQuantityUnit() {
		return usageQuantityUnit;
	}

	public void setUsageQuantityUnit(String usageQuantityUnit) {
		this.usageQuantityUnit = usageQuantityUnit;
	}

	public String getAppliedOfferIndex() {
		return appliedOfferIndex;
	}

	public void setAppliedOfferIndex(String appliedOfferIndex) {
		this.appliedOfferIndex = appliedOfferIndex;
	}

	public String getBalanceUpdateIndex() {
		return balanceUpdateIndex;
	}

	public void setBalanceUpdateIndex(String balanceUpdateIndex) {
		this.balanceUpdateIndex = balanceUpdateIndex;
	}

	public String getUpdateType() {
		return updateType;
	}

	public void setUpdateType(String updateType) {
		this.updateType = updateType;
	}

	public String getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		this.amount = amount;
	}

	public String getImpactSource() {
		return impactSource;
	}

	public void setImpactSource(String impactSource) {
		this.impactSource = impactSource;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}
}
