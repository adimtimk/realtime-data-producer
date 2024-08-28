package com.aptiva.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.aptiva.model.Sales2;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import lombok.extern.log4j.Log4j2;
@Service
@Log4j2
public class SalesService2 {
	@Autowired
	@Qualifier("kafkaSales2Template")
	private KafkaTemplate<String, Sales2> kafkaSalesTemplate;
	@Value("${sales.data.path}")
	private String salesDataPath;
	

	public CompletableFuture<String> publishSalesData(String salesTopic){
		//ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

		try {
			sendToKafka(salesTopic);
		} catch (CsvValidationException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return CompletableFuture.completedFuture("Started Events");
		
	}


	private void sendToKafka(String salesTopic) throws IOException, CsvValidationException {
		// TODO Auto-generated method stub

		CSVReader csvReader = null;
		try {
			csvReader = new CSVReader(new FileReader(salesDataPath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		csvReader.skip(1);
		String[] salesData;
		//AgentID, OfficeID, CarID, CustomerID, Amount (USD)
		
		while((salesData= csvReader.readNext())!=null) {
			Sales2 saleObject = null;
			try {
				saleObject = Sales2.builder()
						.agentId(Integer.valueOf(salesData[1]))
						.officeId(Integer.valueOf(salesData[2]))
						.carID(Integer.valueOf(salesData[3]))
						.customerId(Integer.valueOf(salesData[4]))
						.salesDateTxn(new java.sql.Timestamp((new SimpleDateFormat("yyyy-MM-dd").parse(salesData [5])).getTime()))
						.salesAmount(Double.valueOf(salesData[6])).build();
				
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
			log.info("Current object {} ready to send  saleObject to topic - {}",saleObject.toString(),salesTopic);
			kafkaSalesTemplate.send(salesTopic, saleObject);
			log.info("Current object {} after sleep to saleObject send to topic",LocalDateTime.now());

			try {
				Thread.sleep(8000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	}
	
}