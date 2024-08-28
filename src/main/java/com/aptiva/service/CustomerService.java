package com.aptiva.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.aptiva.model.Customer;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import lombok.extern.log4j.Log4j2;
@Service
@Log4j2
public class CustomerService {
	@Autowired
	@Qualifier("kafkaCustomerTemplate")
	private KafkaTemplate<String, Customer> kafkaCustomerTemplate;
	@Value("${customer.data.path}")
	private String customerDataPath;
	
	public CompletableFuture<String> publishSalesData(String customerTopic){
		//ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

		try {
			sendToKafka(customerTopic);
		} catch (CsvValidationException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return CompletableFuture.completedFuture("Started Customer Events");
		
	}
	
	private void sendToKafka(String customerTopic) throws IOException, CsvValidationException {
		// TODO Auto-generated method stub

		CSVReader csvReader = null;
		try {
			csvReader = new CSVReader(new FileReader(customerDataPath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		csvReader.skip(1);
		String[] customsData;
		//CustomerID, Mobile No, Name, Gender, Age, Nationality, PassportNo, ID No, Home Addres	Lease Start Date, Lease Period

		
		while((customsData= csvReader.readNext())!=null) {
			Customer custObject = null;
			Calendar cal = Calendar.getInstance();  
			try {
				cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(customsData [8]));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
			cal.add(Calendar.DAY_OF_MONTH, Integer.valueOf(customsData[10]));  
			String dateAfter = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());  
			Date end = null;
			try {
				end = new SimpleDateFormat("yyyy-MM-dd").parse(dateAfter);
				log.info("End calculated date is {}-period-{}-startdate-{}",end,customsData[10],customsData [8]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				custObject = Customer.builder()
						.customerId(Integer.valueOf(customsData[0]))
						.mobileNum(customsData[1])
						.name(customsData[2])
						.gender(customsData[3])
						.age(Integer.valueOf(customsData[4]))
						.nationality(customsData[5])
						.passportNum(customsData[6])
						.idNum(customsData[7])
						.leaseStartDate(new java.sql.Timestamp(( new SimpleDateFormat("yyyy-MM-dd").parse(customsData [8])).getTime()))
						.leaseEndDate(new java.sql.Timestamp(end.getTime()))
						.leasePeriod(Integer.valueOf(customsData[10]))
						.city(customsData[11])
						.code(customsData[12])
						.area(customsData[13])
						.build();
						
				
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
			log.info("Current object {} ready to send  custObject to topic - {}",custObject.toString(),customerTopic);
			kafkaCustomerTemplate.send(customerTopic, custObject);
			log.info("Current object {} after sleep to custObject send to topic",LocalDateTime.now());

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	}
	
}
