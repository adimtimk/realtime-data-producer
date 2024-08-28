package com.aptiva.model;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Sales {
//AgentID, OfficeID, CarID, CustomerID, Amount (USD)
	private Integer salesId;
	private LocalDateTime salesDateTxn;
	private Double salesAmount;
	
}
