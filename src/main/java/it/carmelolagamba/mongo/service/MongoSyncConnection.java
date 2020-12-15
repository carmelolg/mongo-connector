package it.carmelolagamba.mongo.service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import it.carmelolagamba.mongo.config.MongoProperties;

@Component
public class MongoSyncConnection implements MongoConnection<MongoClient> {

	private Logger logger = LoggerFactory.getLogger(MongoSyncConnection.class);

	@Autowired
	private MongoProperties mongoProperties;

	@Override
	public MongoClient create() {
		return (mongoProperties.getClusterHost() != null) ? createMongoClientByCluster()
				: createMongoClientByFixedHost();
	}

	private MongoClient createMongoClientByCluster() {
		String clusterConnectionString = String.join("", "mongodb+srv://", mongoProperties.getUser(), ":",
				mongoProperties.getPassword(), "@", mongoProperties.getClusterHost(), "/", mongoProperties.getDbName());

		MongoClientURI uri = new MongoClientURI(clusterConnectionString);
		return new MongoClient(uri);
	}

	private MongoClient createMongoClientByFixedHost() {
		MongoClientOptions options = MongoClientOptions.builder().build();
		List<ServerAddress> servers = new ArrayList<>();

		try {
			int port = Integer.parseInt(mongoProperties.getPort());
			servers = mongoProperties.getHosts().stream().map(host -> {
				return new ServerAddress(host, port);
			}).collect(Collectors.toList());
		} catch (NumberFormatException e) {
			servers = mongoProperties.getHosts().stream().map(host -> {
				return new ServerAddress(host);
			}).collect(Collectors.toList());
		}

		if (servers.isEmpty()) {
			logger.error("Host not found. Please add it on configuration yml.");
			return null;
		}

		if (!mongoProperties.isAuth()) {
			ServerAddress address = servers.stream().findFirst().get();
			return new MongoClient(address, options);
		} else {
			MongoCredential credential = MongoCredential.createCredential(mongoProperties.getUser(),
					mongoProperties.getDbName(), mongoProperties.getPassword().toCharArray());
			return new MongoClient(servers, credential, options);
		}
	}

}
