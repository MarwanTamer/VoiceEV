package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OEGPurchaseEvent {
	@JsonProperty("AccountBrand") 
    public String accountBrand;
    @JsonProperty("SubscriberType") 
    public String subscriberType;
    @JsonProperty("MSISDN") 
    public String mSISDN;
    @JsonProperty("OfferInfoArray") 
    public OfferInfoArray offerInfoArray;
    @JsonProperty("BundleInfoArray") 
    public BundleInfoArray bundleInfoArray;
    @JsonProperty("Reason") 
    public String reason;
    @JsonProperty("Info") 
    public String info;
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
	public OfferInfoArray getOfferInfoArray() {
		return offerInfoArray;
	}
	public void setOfferInfoArray(OfferInfoArray offerInfoArray) {
		this.offerInfoArray = offerInfoArray;
	}
	public BundleInfoArray getBundleInfoArray() {
		return bundleInfoArray;
	}
	public void setBundleInfoArray(BundleInfoArray bundleInfoArray) {
		this.bundleInfoArray = bundleInfoArray;
	}
	public String getReason() {
		return reason;
	}
	public void setReason(String reason) {
		this.reason = reason;
	}
	public String getInfo() {
		return info;
	}
	public void setInfo(String info) {
		this.info = info;
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
