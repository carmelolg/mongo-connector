package it.carmelolagamba.mongo.service;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.List;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.Conventions;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoIterable;

import it.carmelolagamba.mongo.utils.MongoStatusConnection;
import it.carmelolagamba.mongo.utils.MongoStatusConnection.Status;

@Component
public class MongoService {

	private Logger logger = LoggerFactory.getLogger(MongoService.class);

	@Autowired
	private MongoAsyncConnection asyncConnection;

	@Autowired
	private MongoSyncConnection syncConnection;

	private MongoClient mongoClientInstance = null;
	private com.mongodb.async.client.MongoClient asyncMongoClientInstance = null;

	public CodecRegistry getCodecRegistry() {

		List<Convention> conventions = new ArrayList<>();
		conventions.addAll(Conventions.DEFAULT_CONVENTIONS);

		return fromRegistries(MongoClient.getDefaultCodecRegistry(),
				fromProviders(PojoCodecProvider.builder().conventions(conventions).automatic(true).build()));
	}

	public MongoClient getMongoClient() {

		if (mongoClientInstance == null) {
			mongoClientInstance = syncConnection.create();
		}

		return mongoClientInstance;
	}

	public com.mongodb.async.client.MongoClient getAsyncMongoClient() {

		if (asyncMongoClientInstance == null) {
			asyncMongoClientInstance = asyncConnection.create();
		}

		return asyncMongoClientInstance;
	}

	public MongoStatusConnection statusInfo() {
		final String statusName = MongoStatusConnection.Services.MONGODB.getValue();
		MongoStatusConnection statusConnection = new MongoStatusConnection();
		try {
			logger.info("Connection[{}]", MongoStatusConnection.Services.MONGODB.getValue());
			final long startTime = System.currentTimeMillis();
			ping();
			final long elapsedTime = System.currentTimeMillis() - startTime;
			statusConnection.setName(statusName).setStatus(MongoStatusConnection.Status.OK).setMessage("Connection ok")
					.setTimeInMillis(elapsedTime);

			statusConnection.addDetail(replicaStatus());
			logger.info("Connection status {} - {}", MongoStatusConnection.Services.MONGODB.getValue(),
					statusConnection.toString());
		} catch (Exception ex) {
			logger.error("Connection status " + MongoStatusConnection.Services.MONGODB.getValue() + " - KO", ex);
			statusConnection.setName(statusName).setStatus(MongoStatusConnection.Status.KO)
					.setMessage("Connection error - " + ex.getMessage());
		}
		return statusConnection;
	}

	private void ping() {
		DBObject ping = new BasicDBObject("ping", "1");
		MongoIterable<String> dbList = getMongoClient().listDatabaseNames();
		getMongoClient().getDatabase(dbList.first()).runCommand((Bson) ping);
	}

	private MongoStatusConnection replicaStatus() {

		String message = "";
		MongoStatusConnection replicaStatus = new MongoStatusConnection();
		replicaStatus.setName(MongoStatusConnection.Services.MONGO_REPLICA_SET.getValue());

		try {
			final long startTime = System.currentTimeMillis();
			ReplicaSetStatus replicaSetStatus = getMongoClient().getReplicaSetStatus();

			String[] hostParameters = getMongoClient().getConnectPoint().split(":", 2);
			String hostAddress = hostParameters[0];
			Integer hostPort = Integer.parseInt(hostParameters[1]);
			StringBuilder builder = new StringBuilder();

			if (replicaSetStatus == null) {
				builder.append("I'm not currently connected to the mongo replica set");
				builder.append(" Host: " + hostAddress + " Port: " + hostPort);
			} else {
				boolean isMaster = replicaSetStatus.isMaster(new ServerAddress(hostAddress, hostPort));
				builder.append("I'm currently connected to the mongo replica set: " + replicaSetStatus.getName());
				builder.append(" Host: " + hostAddress + " Port: " + hostPort);
				builder.append(" I'm connected to master: " + isMaster);
			}

			final long elapsedTime = System.currentTimeMillis() - startTime;
			replicaStatus.setMessage(builder.toString());
			replicaStatus.setStatus(Status.OK);
			replicaStatus.setTimeInMillis(elapsedTime);

		} catch (NumberFormatException e) {
			message = "[MongoService]: primary host unknown.";
			logger.error(message, e);
			replicaStatus.setMessage(message);
			replicaStatus.setStatus(Status.KO);
		} catch (Exception e) {
			message = "[MongoService]: primary host unknown.";
			logger.error(message, e);
			replicaStatus.setMessage(message);
			replicaStatus.setStatus(Status.KO);
		}

		return replicaStatus;
	}

}
