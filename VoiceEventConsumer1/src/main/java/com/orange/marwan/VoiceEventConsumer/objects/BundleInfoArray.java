package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BundleInfoArray {
	@JsonProperty("MtxPurchaseEventBundleInfo")
	public List<MtxPurchaseEventBundleInfo> mtxPurchaseEventBundleInfo;

	public List<MtxPurchaseEventBundleInfo> getMtxPurchaseEventBundleInfo() {
		return mtxPurchaseEventBundleInfo;
	}

	public void setMtxPurchaseEventBundleInfo(List<MtxPurchaseEventBundleInfo> mtxPurchaseEventBundleInfo) {
		this.mtxPurchaseEventBundleInfo = mtxPurchaseEventBundleInfo;
	}
}
