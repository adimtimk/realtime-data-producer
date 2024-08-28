package com.aptiva.model;

import java.sql.Timestamp;
import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Sales2 {
//AgentID, OfficeID, CarID, CustomerID, Amount (USD)
	private Integer salesId;
	private Integer agentId;
	private Integer officeId;
	private Integer carID;
	private Integer customerId;
	private Timestamp salesDateTxn;
	private Double salesAmount;
	
}
