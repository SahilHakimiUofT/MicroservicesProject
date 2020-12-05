package com.csc301.songmicroservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;

@SpringBootApplication
public class SongMicroserviceApplication {

	public static void main(String[] args) {
		
		SpringApplication.run(SongMicroserviceApplication.class, args);
		
		System.out.println("Song Microservice is running on port 3001");
	}
	
}
