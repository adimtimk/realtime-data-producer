package com.aptiva.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import com.aptiva.model.GPSData;
import com.aptiva.model.GPSData.GPSDataBuilder;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import lombok.extern.log4j.Log4j2;
@Service
@Log4j2

public class GPSService {
	@Autowired
	private KafkaTemplate<String, GPSData> kafkaTemplate;
	@Value("${gps.data.path}")
	private String gpsDataPath;
	
	public CompletableFuture<String> publishGPSData(String gpsTopic){
		//ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

		try {
			sendToKafka(gpsTopic);
			
			return CompletableFuture.completedFuture("Started Events");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return CompletableFuture.completedFuture("Started Events v");
		
	}
	
	private void sendToKafka(String gpsTopic) throws FileNotFoundException, IOException, CsvValidationException {
		CSVReader csvReader = new CSVReader(new FileReader(gpsDataPath));
		csvReader.skip(1);
		String[] gpsData;
		//﻿customerId,carID,officeID,agentID,timeStamp,carMovementStatus,currLongitude,currLatitude,currArea,currKilometers
		
		while((gpsData= csvReader.readNext())!=null) {
			GPSData gpsObject = null;
			try {
				gpsObject = GPSData.builder()
						.customerId(Integer.valueOf(gpsData[0]))
						.carID(Integer.valueOf(gpsData[1]))
						.officeID(Integer.valueOf(gpsData[2]))
						.agentID(Integer.valueOf(gpsData[3]))
						.gtimeStamp( new java.sql.Timestamp((new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(gpsData [4])).getTime()))
						.carMovementStatus(gpsData[5])
						.currLongitude(Double.valueOf(gpsData[6]))
						.currLatitude(Double.valueOf(gpsData[7]))
						.currArea(gpsData[8])
						.currKilometers(Double.valueOf(gpsData[9])).build();
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
					
//				for(String value : gpsData) {
//					
//				}
			log.info("Current object {} ready to send to topic - {}",gpsObject.toString(),gpsTopic);
			kafkaTemplate.send(gpsTopic,gpsObject.getCustomerId().toString(), gpsObject);
			log.info("Current object {} after sleep to send to topic",LocalDateTime.now());

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
