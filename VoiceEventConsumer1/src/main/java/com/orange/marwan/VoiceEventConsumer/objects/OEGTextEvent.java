package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OEGTextEvent {
	@JsonProperty("VLRInfo")
	public String vLRInfo;
	public String sessionIdCust;
	public String ccRequestType;
	public String ccRequestNumber;
	@JsonProperty("BPartyMsisdn")
	public String bPartyMsisdn;
	@JsonProperty("APartyMsisdn")
	public String aPartyMsisdn;
	@JsonProperty("OtherPartyCountry")
	public String otherPartyCountry;
	@JsonProperty("ChargedPartyCountry")
	public String chargedPartyCountry;
	public String originationCCRegion;
	@JsonProperty("Imsi")
	public String imsi;
	@JsonProperty("RoamingFlag")
	public String roamingFlag;
	@JsonProperty("CallUsageType")
	public String callUsageType;
	public String iddCCRegion;
	@JsonProperty("RoamingCarrier")
	public String roamingCarrier;
	public String originationCarrier;
	@JsonProperty("AccountBrand")
	public String accountBrand;
	@JsonProperty("SubscriberType")
	public String subscriberType;
	@JsonProperty("MSISDN")
	public String mSISDN;
	@JsonProperty("ServiceId")
	public String serviceId;
	@JsonProperty("UsageQuantityList")
	public UsageQuantityList usageQuantityList;
	@JsonProperty("UsageUtcOffset")
	public String usageUtcOffset;
	@JsonProperty("LastUsageRoundingAmount")
	public String lastUsageRoundingAmount;
	@JsonProperty("UsageRoundingAmount")
	public String usageRoundingAmount;
	@JsonProperty("UsageRoundingAmountUnit")
	public String usageRoundingAmountUnit;
	@JsonProperty("EventTypeArray")
	public EventTypeArray eventTypeArray;
	@JsonProperty("AppliedOfferArray")
	public AppliedOfferArray appliedOfferArray;
	@JsonProperty("AppliedBundleArray")
	public AppliedBundleArray appliedBundleArray;
	@JsonProperty("AppliedCatalogItemArray")
	public AppliedCatalogItemArray appliedCatalogItemArray;
	@JsonProperty("InitiatorId")
	public String initiatorId;
	@JsonProperty("InitiatorExternalId")
	public String initiatorExternalId;
	@JsonProperty("InitiatorDeviceId")
	public String initiatorDeviceId;
	@JsonProperty("Flags")
	public String flags;
	@JsonProperty("WalletId")
	public String walletId;
	@JsonProperty("WalletOwnerId")
	public String walletOwnerId;
	@JsonProperty("WalletOwnerExternalId")
	public String walletOwnerExternalId;
	@JsonProperty("BalanceUpdateArray")
	public BalanceUpdateArray balanceUpdateArray;
	@JsonProperty("ChargeList")
	public ChargeList chargeList;
	@JsonProperty("EventTime")
	public Date eventTime;
	@JsonProperty("Duration")
	public String duration;
	@JsonProperty("EventId")
	public String eventId;
	@JsonProperty("DeleteCode")
	public String deleteCode;
	@JsonProperty("InitiatorPrimaryUserId")
	public String initiatorPrimaryUserId;
	@JsonProperty("InitiatorType")
	public String initiatorType;
	@JsonProperty("WalletOwnerType")
	public String walletOwnerType;

	public String getvLRInfo() {
		return vLRInfo;
	}

	public void setvLRInfo(String vLRInfo) {
		this.vLRInfo = vLRInfo;
	}

	public String getSessionIdCust() {
		return sessionIdCust;
	}

	public void setSessionIdCust(String sessionIdCust) {
		this.sessionIdCust = sessionIdCust;
	}

	public String getCcRequestType() {
		return ccRequestType;
	}

	public void setCcRequestType(String ccRequestType) {
		this.ccRequestType = ccRequestType;
	}

	public String getCcRequestNumber() {
		return ccRequestNumber;
	}

	public void setCcRequestNumber(String ccRequestNumber) {
		this.ccRequestNumber = ccRequestNumber;
	}

	public String getbPartyMsisdn() {
		return bPartyMsisdn;
	}

	public void setbPartyMsisdn(String bPartyMsisdn) {
		this.bPartyMsisdn = bPartyMsisdn;
	}

	public String getaPartyMsisdn() {
		return aPartyMsisdn;
	}

	public void setaPartyMsisdn(String aPartyMsisdn) {
		this.aPartyMsisdn = aPartyMsisdn;
	}

	public String getOtherPartyCountry() {
		return otherPartyCountry;
	}

	public void setOtherPartyCountry(String otherPartyCountry) {
		this.otherPartyCountry = otherPartyCountry;
	}

	public String getChargedPartyCountry() {
		return chargedPartyCountry;
	}

	public void setChargedPartyCountry(String chargedPartyCountry) {
		this.chargedPartyCountry = chargedPartyCountry;
	}

	public String getOriginationCCRegion() {
		return originationCCRegion;
	}

	public void setOriginationCCRegion(String originationCCRegion) {
		this.originationCCRegion = originationCCRegion;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getRoamingFlag() {
		return roamingFlag;
	}

	public void setRoamingFlag(String roamingFlag) {
		this.roamingFlag = roamingFlag;
	}

	public String getCallUsageType() {
		return callUsageType;
	}

	public void setCallUsageType(String callUsageType) {
		this.callUsageType = callUsageType;
	}

	public String getIddCCRegion() {
		return iddCCRegion;
	}

	public void setIddCCRegion(String iddCCRegion) {
		this.iddCCRegion = iddCCRegion;
	}

	public String getRoamingCarrier() {
		return roamingCarrier;
	}

	public void setRoamingCarrier(String roamingCarrier) {
		this.roamingCarrier = roamingCarrier;
	}

	public String getOriginationCarrier() {
		return originationCarrier;
	}

	public void setOriginationCarrier(String originationCarrier) {
		this.originationCarrier = originationCarrier;
	}

	public String getAccountBrand() {
		return accountBrand;
	}

	public void setAccountBrand(String accountBrand) {
		this.accountBrand = accountBrand;
	}

	public String getSubscriberType() {
		return subscriberType;
	}

	public void setSubscriberType(String subscriberType) {
		this.subscriberType = subscriberType;
	}

	public String getmSISDN() {
		return mSISDN;
	}

	public void setmSISDN(String mSISDN) {
		this.mSISDN = mSISDN;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public UsageQuantityList getUsageQuantityList() {
		return usageQuantityList;
	}

	public void setUsageQuantityList(UsageQuantityList usageQuantityList) {
		this.usageQuantityList = usageQuantityList;
	}

	public String getUsageUtcOffset() {
		return usageUtcOffset;
	}

	public void setUsageUtcOffset(String usageUtcOffset) {
		this.usageUtcOffset = usageUtcOffset;
	}

	public String getLastUsageRoundingAmount() {
		return lastUsageRoundingAmount;
	}

	public void setLastUsageRoundingAmount(String lastUsageRoundingAmount) {
		this.lastUsageRoundingAmount = lastUsageRoundingAmount;
	}

	public String getUsageRoundingAmount() {
		return usageRoundingAmount;
	}

	public void setUsageRoundingAmount(String usageRoundingAmount) {
		this.usageRoundingAmount = usageRoundingAmount;
	}

	public String getUsageRoundingAmountUnit() {
		return usageRoundingAmountUnit;
	}

	public void setUsageRoundingAmountUnit(String usageRoundingAmountUnit) {
		this.usageRoundingAmountUnit = usageRoundingAmountUnit;
	}

	public EventTypeArray getEventTypeArray() {
		return eventTypeArray;
	}

	public void setEventTypeArray(EventTypeArray eventTypeArray) {
		this.eventTypeArray = eventTypeArray;
	}

	public AppliedOfferArray getAppliedOfferArray() {
		return appliedOfferArray;
	}

	public void setAppliedOfferArray(AppliedOfferArray appliedOfferArray) {
		this.appliedOfferArray = appliedOfferArray;
	}

	public AppliedBundleArray getAppliedBundleArray() {
		return appliedBundleArray;
	}

	public void setAppliedBundleArray(AppliedBundleArray appliedBundleArray) {
		this.appliedBundleArray = appliedBundleArray;
	}

	public AppliedCatalogItemArray getAppliedCatalogItemArray() {
		return appliedCatalogItemArray;
	}

	public void setAppliedCatalogItemArray(AppliedCatalogItemArray appliedCatalogItemArray) {
		this.appliedCatalogItemArray = appliedCatalogItemArray;
	}

	public String getInitiatorId() {
		return initiatorId;
	}

	public void setInitiatorId(String initiatorId) {
		this.initiatorId = initiatorId;
	}

	public String getInitiatorExternalId() {
		return initiatorExternalId;
	}

	public void setInitiatorExternalId(String initiatorExternalId) {
		this.initiatorExternalId = initiatorExternalId;
	}

	public String getInitiatorDeviceId() {
		return initiatorDeviceId;
	}

	public void setInitiatorDeviceId(String initiatorDeviceId) {
		this.initiatorDeviceId = initiatorDeviceId;
	}

	public String getFlags() {
		return flags;
	}

	public void setFlags(String flags) {
		this.flags = flags;
	}

	public String getWalletId() {
		return walletId;
	}

	public void setWalletId(String walletId) {
		this.walletId = walletId;
	}

	public String getWalletOwnerId() {
		return walletOwnerId;
	}

	public void setWalletOwnerId(String walletOwnerId) {
		this.walletOwnerId = walletOwnerId;
	}

	public String getWalletOwnerExternalId() {
		return walletOwnerExternalId;
	}

	public void setWalletOwnerExternalId(String walletOwnerExternalId) {
		this.walletOwnerExternalId = walletOwnerExternalId;
	}

	public BalanceUpdateArray getBalanceUpdateArray() {
		return balanceUpdateArray;
	}

	public void setBalanceUpdateArray(BalanceUpdateArray balanceUpdateArray) {
		this.balanceUpdateArray = balanceUpdateArray;
	}

	public ChargeList getChargeList() {
		return chargeList;
	}

	public void setChargeList(ChargeList chargeList) {
		this.chargeList = chargeList;
	}

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getDeleteCode() {
		return deleteCode;
	}

	public void setDeleteCode(String deleteCode) {
		this.deleteCode = deleteCode;
	}

	public String getInitiatorPrimaryUserId() {
		return initiatorPrimaryUserId;
	}

	public void setInitiatorPrimaryUserId(String initiatorPrimaryUserId) {
		this.initiatorPrimaryUserId = initiatorPrimaryUserId;
	}

	public String getInitiatorType() {
		return initiatorType;
	}

	public void setInitiatorType(String initiatorType) {
		this.initiatorType = initiatorType;
	}

	public String getWalletOwnerType() {
		return walletOwnerType;
	}

	public void setWalletOwnerType(String walletOwnerType) {
		this.walletOwnerType = walletOwnerType;
	}
}
