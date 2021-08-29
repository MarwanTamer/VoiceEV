package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AppliedCatalogItemArray {
	@JsonProperty("MtxEventAppliedCatalogItem")
	public List<MtxEventAppliedCatalogItem> mtxEventAppliedCatalogItem;

	public List<MtxEventAppliedCatalogItem> getMtxEventAppliedCatalogItem() {
		return mtxEventAppliedCatalogItem;
	}

	public void setMtxEventAppliedCatalogItem(List<MtxEventAppliedCatalogItem> mtxEventAppliedCatalogItem) {
		this.mtxEventAppliedCatalogItem = mtxEventAppliedCatalogItem;
	}
}
