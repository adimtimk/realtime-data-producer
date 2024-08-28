package com.aptiva.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.aptiva.batch.CustomGPSDataProcessor;
import com.aptiva.model.GPSData;

import lombok.AllArgsConstructor;

@Configuration
@AllArgsConstructor
@EnableBatchProcessing
public class SpringBatchConfig {

	
	@Value("${gps.data.path}")
	private String gpsDataPath;

	
	
	@Autowired
	private KafkaTemplate<String, GPSData> gpsdataKafkaTemplate;


	@Autowired
	private PlatformTransactionManager transactionManager;
	
	@Bean
	FlatFileItemReader<GPSData> reader(){
		FlatFileItemReader<GPSData> gpsReader =  new FlatFileItemReader<>();
		gpsReader.setResource(new FileSystemResource(gpsDataPath));
		gpsReader.setName("GPS DATA READER");
		gpsReader.setLinesToSkip(1);
		gpsReader.setLineMapper(lineMapper());
		
		
		return gpsReader;
		
		
	}
	
	private LineMapper<GPSData> lineMapper() {
		DefaultLineMapper<GPSData> lmapper =  new DefaultLineMapper<>();
		
		DelimitedLineTokenizer delimiter = new DelimitedLineTokenizer();
		
		delimiter.setDelimiter(",");
		delimiter.setStrict(false);
		delimiter.setNames("customerId","carID","officeID","agentID","timeStamp" ,"carMovementStatus","currLongitude","currLatitude","currArea","currKilometers");
		
		
		BeanWrapperFieldSetMapper<GPSData> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(GPSData.class);
		// TODO Auto-generated method stub
		
		lmapper.setLineTokenizer(delimiter);
		lmapper.setFieldSetMapper(fieldSetMapper);
		
		
		return lmapper;
	}
	
	
	@Bean
	CustomGPSDataProcessor processor() {
		return new CustomGPSDataProcessor();
	}
	
	@Bean
	KafkaItemWriter<String , GPSData> kafkaWriter(){
		KafkaItemWriter<String, GPSData> kafkaItemWriter= new KafkaItemWriter<String , GPSData>();
		kafkaItemWriter.setKafkaTemplate(gpsdataKafkaTemplate);
		kafkaItemWriter.setItemKeyMapper(new Converter<GPSData, String>() {

			@Override
			public String convert(GPSData source) {
				// TODO Auto-generated method stub
				return source.getCustomerId().toString();
			}
			
		});
		kafkaItemWriter.setDelete(false);
		
		return kafkaItemWriter;
		
	}
	
	  @Bean
	   Step stepReadFromFileToKafka(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
	        return new StepBuilder("csv_steps", jobRepository)
	                 .<GPSData,GPSData>chunk(10, transactionManager)
	                 .reader(reader())
	                 .processor(processor())
	                 .writer(kafkaWriter())
	                 .taskExecutor(batchtaskExecutor())
	                .build();
	    }
	
//	@Bean
//	Step stepReadFromFileToKafka() throws Exception {
//		return stepBuilderFactory.get(gpsDataPath).<GPSData,GPSData>chunk(10).reader(reader()).processor(processor()).writer(kafkaWriter()).taskExecutor(batchtaskExecutor()).build();
//		
////		return new StepBuilderFactory.("csv-kafka",simpleJobRepository()).<GPSData,GPSData>chunk(10,null).
////				reader(reader()).processor(processor()).writer(kafkaWriter()).taskExecutor(batchtaskExecutor()).build();
//	}
	
//	private JobRepository simpleJobRepository() throws Exception {
//		// TODO Auto-generated method stub
//	    JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
//	    factory.setIncrementerFactory((DataFieldMaxValueIncrementerFactory) new DefaultDataFieldMaxValueIncrementerFactory(dataSource) {
//			public DataFieldMaxValueIncrementer getIncrementer(String incrementerType, String incrementerName) {
//				return new MySQLMaxValueIncrementer();
//			}
//		});
//		return factory.getObject() ;
//	}

//	@Bean
//	Job runJob() throws Exception {
//		
////		return new JobBuilder("csv-kafka-job",simpleJobRepository()).flow(stepReadFromFileToKafka()).end().build();
//		return jobBuilderFactory.get("CSV-Kafka-ETL-Load")
//				.incrementer(new RunIdIncrementer())
//				.start(stepReadFromFileToKafka())
//				.build();
//	}
//	
	 	@Bean
	     Job myJob(JobRepository jobRepository, Step step) {
	        return new JobBuilder("myJob", jobRepository)
	                .start(stepReadFromFileToKafka(jobRepository, transactionManager))
	                .incrementer(new RunIdIncrementer())
	                .build();
	    }
	 
	@Bean
	TaskExecutor batchtaskExecutor() {
		SimpleAsyncTaskExecutor sAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
		sAsyncTaskExecutor.setConcurrencyLimit(10);
		return sAsyncTaskExecutor;
	}
}
