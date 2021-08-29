package com.orange.marwan.VoiceEventConsumer.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BalanceUpdateArray {
	@JsonProperty("MtxBalanceUpdate")
	public List<MtxBalanceUpdate> mtxBalanceUpdate;

	public List<MtxBalanceUpdate> getMtxBalanceUpdate() {
		return mtxBalanceUpdate;
	}

	public void setMtxBalanceUpdate(List<MtxBalanceUpdate> mtxBalanceUpdate) {
		this.mtxBalanceUpdate = mtxBalanceUpdate;
	}
}
