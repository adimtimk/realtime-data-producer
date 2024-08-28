package com.aptiva.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.aptiva.model.Sales;
import com.aptiva.model.Sales2;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import lombok.extern.log4j.Log4j2;
@Service
@Log4j2
public class SalesService {
	@Autowired
	@Qualifier("kafkaSalesTemplate")
	private KafkaTemplate<String, Sales> kafkaSalesTemplate;
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
			Sales saleObject = null;
			try {
				saleObject = Sales.builder()
						.salesId(Integer.valueOf(salesData[0]))
						.salesDateTxn(LocalDateTime.parse(salesData[1],DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
						.salesAmount(Double.valueOf(salesData[2]))
						.build();
				
			} catch (NumberFormatException e) {
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