/*******************************************************************************
 * Copyright 2018 572682
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package us.dot.its.jpo.ode.wrapper;

import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 572682
 * This class encapsulates a message produce function. 
 * 
 * @param <K> Message Key type
 * @param <V> Message Value type
 */
public class MessageProducer<K, V> {
    public static final String SERIALIZATION_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String SERIALIZATION_BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final int DEFAULT_PRODUCER_BUFFER_MEMORY_BYTES = 33554432;
    public static final int DEFAULT_PRODUCER_LINGER_MS = 1;
    public static final int DEFAULT_PRODUCER_BATCH_SIZE_BYTES = 16384;
    public static final int DEFAULT_PRODUCER_RETRIES = 0;
    public static final String DEFAULT_PRODUCER_ACKS = "all";

    private static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private Producer<K, V> producer;
    private Set<String> disabledTopicsSet;

    private static KAFKA_CONNECTION connectionType = KAFKA_CONNECTION.LOCAL_INSTANCE;

    public MessageProducer(
        String brokers, 
        String type, 
        String partitionerClass, 
        String valueSerializerFQN,
        Set<String> disabledTopics) {
        Properties props = setDefaultProperties();
        
        if(brokers != null) 
        {
        	props.put("bootstrap.servers", brokers);
        }
        else 
        {
        	logger.error("Bootstrap servers setting is null");
        }
        
        props.put("key.serializer", SERIALIZATION_STRING_SERIALIZER);
        props.put("value.serializer", valueSerializerFQN);

        if (partitionerClass != null)
            props.put("partitioner.class", partitionerClass);

        producer = new KafkaProducer<>(props);
        
        this.disabledTopicsSet = disabledTopics;

        logger.info("Producer Created with default properties");
    }

    public MessageProducer(
       String brokers, String type, String partitionerClass, Properties props,
       Set<String> enabledTopics) {
        props.put("bootstrap.servers", brokers);
        if (partitionerClass != null)
            props.put("partitioner.class", partitionerClass);

        producer = new KafkaProducer<>(props);

        this.disabledTopicsSet = enabledTopics;

        logger.info("Producer Created");
    }

    public static MessageProducer<String, byte[]> defaultByteArrayMessageProducer(String brokers, String type,
       Set<String> disabledTopics) {
        return new MessageProducer<String, byte[]>(brokers, type, null, SERIALIZATION_BYTE_ARRAY_SERIALIZER, disabledTopics);
    }

    public static MessageProducer<String, String> defaultStringMessageProducer(String brokers, String type,
       Set<String> disabledTopics) {
        return new MessageProducer<String, String>(brokers, type, null, SERIALIZATION_STRING_SERIALIZER, disabledTopics);
    }

    public void send(String topic, K key, V value) {
       if (!disabledTopicsSet.contains(topic)) {
          ProducerRecord<K, V> data;
          if (key == null)
              data = new ProducerRecord<>(topic, value);
          else
              data = new ProducerRecord<>(topic, key, value);

          producer.send(data, new Callback() {

             @Override
             public void onCompletion(RecordMetadata returnMetadata, Exception e) {
                if (null != e) {
                   logger.error("Error sending record.", e);
                } else {
                   logger.debug("Completed publish to topic: {}, offset: {}, partition: {}", returnMetadata.topic(), returnMetadata.offset(), returnMetadata.partition());
                }
                
             }
               
            });
       }
    }

    public void close() {
        producer.close();
        logger.info("Producer Closed");
    }

    public Producer<K, V> getProducer() {
        return producer;
    }

    public MessageProducer<K, V> setProducer(Producer<K, V> producer) {
        this.producer = producer;
        return this;
    }

   public void send(ProducerRecord<K, V> producerRecord) {
        producer.send(producerRecord, new Callback() {

         @Override
         public void onCompletion(RecordMetadata returnMetadata, Exception e) {
            if (null != e) {
               logger.error("Error sending record.", e);
            } else {
               logger.debug("Record metadata: {}", returnMetadata);
            }
            
         }
           
        });

    }

    /**
     * @author Daniel McCoy Stephenson
     * @return The currently specified type of connection for Kafka.
     */
    public static KAFKA_CONNECTION getConnectionType() {
        return connectionType;
    }

    /**
     * @author Daniel McCoy Stephenson
     * @param newConnectionType The new type of connection for Kafka.
     */
    public static void setConnectionType(KAFKA_CONNECTION newConnectionType) {
        connectionType = newConnectionType;
    }

    /**
     * @author Daniel McCoy Stephenson
     */
    private static Properties setDefaultProperties() {
        switch(connectionType) {
            case LOCAL_INSTANCE:
                return setDefaultPropertiesForLocalKafkaInstance();
            case CONFLUENT_CLOUD:
                String APIKey = null; // TODO: introduce environment variable for this
                String APISecret = null; // TODO: introduce environment variable for this
                return setDefaultPropertiesForConfluentCloud(APIKey, APISecret);
            default:
                return null;
        }
    }

    /**
     * @author Daniel McCoy Stephenson
     * @return Properties for a local Kafka instance.
     */
    private static Properties setDefaultPropertiesForLocalKafkaInstance() {
        // NOSONAR
        Properties props = new Properties();
        
        props.put("acks", DEFAULT_PRODUCER_ACKS); // Set acknowledgments for producer requests.
        props.put("retries", DEFAULT_PRODUCER_RETRIES); // If the request fails, the producer can automatically retry
        props.put("batch.size", DEFAULT_PRODUCER_BATCH_SIZE_BYTES);
        props.put("linger.ms", DEFAULT_PRODUCER_LINGER_MS);

        // The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", DEFAULT_PRODUCER_BUFFER_MEMORY_BYTES);
        return props;
    }

    /**
     * @author Daniel McCoy Stephenson
     * @param clusterAPIKey The API key for the cluster.
     * @param clusterAPISecret The API secret for the cluster.
     * @return Properties for a Confluent Cloud connection.
     */
    private static Properties setDefaultPropertiesForConfluentCloud(String clusterAPIKey, String clusterAPISecret) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "pkc-43n10.us-central1.gcp.confluent.cloud:9092");
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=" + clusterAPIKey + " password=" + clusterAPISecret + ";");
        return properties;
    }

    /**
     * @author Daniel McCoy Stephenson
     */
    private enum KAFKA_CONNECTION {
        LOCAL_INSTANCE,
        CONFLUENT_CLOUD
    }
}