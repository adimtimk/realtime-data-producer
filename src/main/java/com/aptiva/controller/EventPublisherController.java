package com.aptiva.controller;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aptiva.service.CustomerService;
import com.aptiva.service.GPSService;
import com.aptiva.service.SalesService;
import com.aptiva.service.SalesService2;

@RestController
@RequestMapping(path = "/api/v1")
public class EventPublisherController {
	
	@Value("${gps.topic}")
    private String gps_topic;
	
	@Value("${sales.topic}")
    private String sales_topic;
	
	@Value("${customer.topic}")
    private String customer_topic;
	
	@Autowired
	private GPSService gpsService;
	
	@Autowired
	private SalesService2 salesService;
	
	@Autowired
	private SalesService salesp;
	
	@Autowired
	private CustomerService customService;
	
	@PostMapping(value = "/gps")
	public CompletableFuture<String> generateCarGpsData(String gpsTopic) {
			
		return gpsService.publishGPSData(gps_topic);
		
	}

	@PostMapping(value = "/sales")
	public CompletableFuture<String> generateSalesTxns() {
		return salesService.publishSalesData(sales_topic);
		
	}
	
	@PostMapping(value = "/salesdummy")
	public CompletableFuture<String> generateSales() {
		return salesp.publishSalesData(sales_topic);
		
	}
	
	@PostMapping(value = "/customers")
	public CompletableFuture<String> generateCustomerData() {
		return customService.publishSalesData(customer_topic);
		
	}
}
