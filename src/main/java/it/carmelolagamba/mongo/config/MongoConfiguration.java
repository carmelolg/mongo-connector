package it.carmelolagamba.mongo.config;

import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

@Configuration
@EnableConfigurationProperties({ MongoProperties.class })
public class MongoConfiguration {

	static Logger mongoDriver = (Logger) LoggerFactory.getLogger("org.mongodb.driver");
	static Logger mongoLib = (Logger) LoggerFactory.getLogger("it.carmelolagamba.mongo");

	static {
		mongoDriver.setLevel(Level.ERROR);
		mongoLib.setLevel(Level.ERROR);
	}
}
