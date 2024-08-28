package com.aptiva.batch;

import org.springframework.batch.item.ItemProcessor;

import com.aptiva.model.GPSData;

public class CustomGPSDataProcessor implements ItemProcessor<GPSData, GPSData> {

	@Override
	public GPSData process(GPSData item) throws Exception {
		// TODO Auto-generated method stub
		
		return item;
	}

}
