package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UsageQuantityList {
	public List<MtxEventUsageQuantity> mtxEventUsageQuantity;

	public List<MtxEventUsageQuantity> getMtxEventUsageQuantity() {
		return mtxEventUsageQuantity;
	}

	public void setMtxEventUsageQuantity(List<MtxEventUsageQuantity> mtxEventUsageQuantity) {
		this.mtxEventUsageQuantity = mtxEventUsageQuantity;
	}
}
