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
public class GPSData {
	Integer gpsTxnID;
	Integer customerId;
	Integer carID;
	Integer officeID;
	Integer agentID;
	Timestamp gtimeStamp;
	String carMovementStatus;
	Double currLongitude;
	Double currLatitude;
	String currArea;
	Double currKilometers;
}
