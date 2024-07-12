package com.bdreader.bdreader.Mongo;

import com.bdreader.bdreader.rabbitmq.RabbitMQSender;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class MongoChangeStreamService {
    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    @Value("${spring.data.mongodb.collectionTasks}")
    private String collectionTasks;

    @Value("${spring.data.mongodb.collectionGroups}")
    private String collectionGroups;

    private final RabbitMQSender rabbitMQSender;

    private static final Logger logger = LoggerFactory.getLogger(MongoChangeStreamService.class);

    public MongoChangeStreamService(RabbitMQSender rabbitMQSender) {
        this.rabbitMQSender = rabbitMQSender;
    }

    @PostConstruct
    public void init() {
        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(databaseName);

        MongoCollection<Document> collectionTs = database.getCollection(collectionTasks);
        MongoCollection<Document> collectionGr = database.getCollection(collectionGroups);

        watchCollection(collectionTs);
        watchCollection(collectionGr);
    }

    private void watchCollection(MongoCollection<Document> collection) {
        Flux.from(collection.watch())
                .doOnNext(changeStreamDocument -> {
                    logger.info("Received change stream document: {}", changeStreamDocument);
                })
                .filter(changeStreamDocument -> {
                    OperationType operationType = changeStreamDocument.getOperationType();
                    if (operationType == OperationType.UPDATE) {
                        UpdateDescription updateDescription = changeStreamDocument.getUpdateDescription();
                        if (updateDescription != null) {
                            Set<String> updatedFields = updateDescription.getUpdatedFields().keySet();
                            return updatedFields.contains("dateTimeNotification") || updatedFields.contains("notification") || updatedFields.contains("executor");
                        }
                    } else if (operationType == OperationType.REPLACE || operationType == OperationType.INSERT) {
                        Document fullDocument = changeStreamDocument.getFullDocument();
                        if (fullDocument != null) {
                            return fullDocument.containsKey("dateTimeNotification") && fullDocument.containsKey("notification");
                        }
                    }
                    return false;
                })
                .doOnNext(changeStreamDocument -> {
                    Document document = changeStreamDocument.getFullDocument();
                    if (document != null) {

                        if (document.containsKey("dateTimeNotification") && document.containsKey("notification")) {
                            Boolean notification = document.getBoolean("notification");
                            if (notification != null && notification) {
                                Date notificationTimeDate = document.getDate("dateTimeNotification");
                                Instant instant = notificationTimeDate.toInstant();
                                LocalDateTime notificationTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                                long delay = notificationTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() - System.currentTimeMillis();

                                logger.info("Notification time: {}", notificationTime);
                                logger.info("Current time: {}", LocalDateTime.now());
                                logger.info("Delay: {} milliseconds", delay);


                                if (delay >= 60000) {
                                    logger.info("Sending document to RabbitMQ: {}", document.toJson());
                                    rabbitMQSender.send(document.toJson(), notificationTime).subscribe();
                                } else {
                                    logger.warn("Notification time is not within the acceptable range (not within the next minute): {}", document.toJson());
                                }
                            } else {
                                logger.warn("Notification field is not true: {}", document.toJson());
                            }

                            if (!document.containsKey("executor") || document.getString("executor").isEmpty()) {
                                logger.warn("Executor field is empty or not present: {}", document.toJson());
                            }
                        } else {
                            logger.warn("Document does not contain 'dateTimeNotification' or 'notification' field: {}", document.toJson());
                        }
                    } else {
                        logger.warn("Received change stream document with no full document: {}", changeStreamDocument);
                    }
                })
                .doOnError(error -> logger.error("Error processing change stream document", error))
                .subscribe();
    }
}
