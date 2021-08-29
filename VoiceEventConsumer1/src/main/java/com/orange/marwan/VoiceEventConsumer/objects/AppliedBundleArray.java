package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AppliedBundleArray {
	@JsonProperty("MtxEventAppliedBundle")
	public List<MtxEventAppliedBundle> mtxEventAppliedBundle;

	public List<MtxEventAppliedBundle> getMtxEventAppliedBundle() {
		return mtxEventAppliedBundle;
	}

	public void setMtxEventAppliedBundle(List<MtxEventAppliedBundle> mtxEventAppliedBundle) {
		this.mtxEventAppliedBundle = mtxEventAppliedBundle;
	}
}
