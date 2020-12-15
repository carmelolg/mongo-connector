package it.carmelolagamba.mongo.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import it.carmelolagamba.mongo.config.MongoProperties;

@Component
public class MongoAsyncConnection implements MongoConnection<MongoClient> {

	@Autowired
	private MongoProperties mongoProperties;

	@Override
	public MongoClient create() {
		return (mongoProperties.getClusterHost() != null) ? createMongoClientByCluster()
				: createMongoClientByFixedHost();
	}

	public com.mongodb.async.client.MongoClient getAsyncMongoClient() {
		return (mongoProperties.getClusterHost() != null) ? createMongoClientByCluster()
				: createMongoClientByFixedHost();
	}

	private MongoClient createMongoClientByFixedHost() {
		List<String> hostsPrepare = mongoProperties.getHosts().stream()
				.map(h -> h.concat(":" + mongoProperties.getPort())).collect(Collectors.toList());
		String hosts = String.join(",", hostsPrepare);

		ConnectionString connectionString = new ConnectionString(
				"mongodb://" + hosts + "/?authSource=" + mongoProperties.getDbName());

		MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

		if (mongoProperties.isAuth()) {
			MongoCredential credential = MongoCredential.createCredential(mongoProperties.getUser(),
					mongoProperties.getDbName(), mongoProperties.getPassword().toCharArray());

			settings = MongoClientSettings.builder().applyConnectionString(connectionString).credential(credential)
					.build();
		}

		return MongoClients.create(settings);
	}

	private MongoClient createMongoClientByCluster() {
		String clusterConnectionString = String.join("", "mongodb+srv://", mongoProperties.getUser(), ":",
				mongoProperties.getPassword(), "@", mongoProperties.getClusterHost(), "/", mongoProperties.getDbName());

		ConnectionString connectionString = new ConnectionString(clusterConnectionString);
		EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
		MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString)
				.streamFactoryFactory(NettyStreamFactoryFactory.builder().eventLoopGroup(eventLoopGroup).build())
				.build();

		return MongoClients.create(settings);
	}
}
