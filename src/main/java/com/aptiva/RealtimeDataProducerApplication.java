package com.aptiva;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class RealtimeDataProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RealtimeDataProducerApplication.class, args);
	}

}
