/*
 * Decompiled with CFR 0_115.
 * 
 * Could not load the following classes:
 *  com.google.common.collect.ImmutableMap
 *  com.rabbitmq.client.BasicProperties
 *  com.rabbitmq.client.Envelope
 *  com.rabbitmq.client.LongString
 *  org.apache.kafka.connect.data.Schema
 *  org.apache.kafka.connect.data.SchemaBuilder
 *  org.apache.kafka.connect.data.Timestamp
 *  org.apache.kafka.connect.errors.DataException
 *  org.apache.kafka.connect.header.Headers
 *  org.slf4j.Logger
 *  org.slf4j.LoggerFactory
 */
package io.confluent.connect.rabbitmq;

import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessageConverter {
    static final Map<Class<?>, Schema> FIELD_LOOKUP;
    static final String HEADER_PREFIX = "rabbitmq.";
    static final String HEADER_CONTENTTYPE = "rabbitmq.content.type";
    static final String HEADER_CONSUMER_TAG = "rabbitmq.consumer.tag";
    static final String HEADER_CONTENTENCODING = "rabbitmq.content.encoding";
    static final String HEADER_HEADERS_PREFIX = "rabbitmq.headers.";
    static final String HEADER_DELIVERYMODE = "rabbitmq.delivery.mode";
    static final String HEADER_PRIORITY = "rabbitmq.priority";
    static final String HEADER_CORRELATIONID = "rabbitmq.correlation.id";
    static final String HEADER_REPLYTO = "rabbitmq.reply.to";
    static final String HEADER_EXPIRATION = "rabbitmq.expiration";
    static final String HEADER_MESSAGEID = "rabbitmq.message.id";
    static final String HEADER_TIMESTAMP = "rabbitmq.timestamp";
    static final String HEADER_TYPE = "rabbitmq.type";
    static final String HEADER_USERID = "rabbitmq.user.id";
    static final String HEADER_APPID = "rabbitmq.app.id";
    static final String HEADER_DELIVERYTAG = "rabbitmq.delivery.tag";
    static final String HEADER_ISREDELIVER = "rabbitmq.redeliver";
    static final String HEADER_EXCHANGE = "rabbitmq.exchange";
    static final String HEADER_ROUTINGKEY = "rabbitmq.routing.key";
    private static final Logger log;

    MessageConverter() {
    }

    static void envelope(Envelope envelope, Headers headers) {
        headers.addLong("rabbitmq.delivery.tag", envelope.getDeliveryTag());
        headers.addBoolean("rabbitmq.redeliver", envelope.isRedeliver());
        headers.addString("rabbitmq.exchange", envelope.getExchange());
        headers.addString("rabbitmq.routing.key", envelope.getRoutingKey());
    }

    static void headers(BasicProperties basicProperties, Headers headers) {
        Map input = basicProperties.getHeaders();
        if (null != input) {
            for (Object o : input.entrySet()) {
            	Map.Entry kvp = (Map.Entry)o;
                Object headerValue;
                String headerKey = "rabbitmq.headers." + (String)kvp.getKey();
                log.trace("headers() - key = '{}' value= '{}'", kvp.getKey(), kvp.getValue());
                if (kvp.getValue() instanceof LongString) {
                    headerValue = kvp.getValue().toString();
                } else if (kvp.getValue() instanceof List) {
                    List list = (List)kvp.getValue();
                    ArrayList<String> values = new ArrayList<String>(list.size());
                    for (Object oo : list) {
                    	LongString l = (LongString)oo;	
                        values.add(l.toString());
                    }
                    headerValue = values;
                } else {
                    headerValue = kvp.getValue();
                }
                if (FIELD_LOOKUP.containsKey(headerValue.getClass())) {
                    Schema schema = FIELD_LOOKUP.get(headerValue.getClass());
                    log.trace("headers() - Storing value for header in field = '{}' as {}", headerValue, (Object)headerKey);
                    headers.add(headerKey, headerValue, schema);
                    continue;
                }
                
                //throw new DataException(String.format("Could not determine the type for field '%s' type '%s'", kvp.getKey(), headerValue.getClass().getName()));
                throw new IllegalStateException(String.format("Could not determine the type for field '%s' type '%s'", kvp.getKey(), headerValue.getClass().getName()));
                
            }
        }
    }

    static void basicProperties(BasicProperties basicProperties, Headers headers) {
        if (null == basicProperties) {
            log.trace("basicProperties() - basicProperties is null.");
            return;
        }
        
        headers.addString("rabbitmq.content.type", basicProperties.getContentType());
        headers.addString("rabbitmq.content.encoding", basicProperties.getContentEncoding());
        
        // path until official library works. -RH
        if(basicProperties.getDeliveryMode() == null) {
        	log.warn("basicProperties.getDeliveryMode() returned null. setting 0 as value.");
        	headers.addInt("rabbitmq.delivery.mode", 0);
        } else {
        	headers.addInt("rabbitmq.delivery.mode", basicProperties.getDeliveryMode().intValue());
        }

        // path until official library works. -RH
        if(basicProperties.getPriority() == null) {
        	log.warn("basicProperties.getPriority() returned null. setting 0 as value.");
        	headers.addInt("rabbitmq.priority", 0);
        } else {
        	headers.addInt("rabbitmq.priority", basicProperties.getPriority().intValue());
        }
        
        headers.addString("rabbitmq.correlation.id", basicProperties.getCorrelationId());
        headers.addString("rabbitmq.reply.to", basicProperties.getReplyTo());
        headers.addString("rabbitmq.expiration", basicProperties.getExpiration());
        headers.addString("rabbitmq.message.id", basicProperties.getMessageId());
        headers.addTimestamp("rabbitmq.timestamp", basicProperties.getTimestamp());
        headers.addString("rabbitmq.type", basicProperties.getType());
        headers.addString("rabbitmq.user.id", basicProperties.getUserId());
        headers.addString("rabbitmq.app.id", basicProperties.getAppId());

    }

    public static void consumerTag(String consumerTag, Headers headers) {
        headers.addString("rabbitmq.consumer.tag", consumerTag);
    }

    static {
        log = LoggerFactory.getLogger((Class)MessageConverter.class);
        HashMap<Class, Schema> fieldLookup = new HashMap<Class, Schema>();
        fieldLookup.put(String.class, Schema.STRING_SCHEMA);
        fieldLookup.put(Byte.class, Schema.INT8_SCHEMA);
        fieldLookup.put(Short.class, Schema.INT16_SCHEMA);
        fieldLookup.put(Integer.class, Schema.INT32_SCHEMA);
        fieldLookup.put(Long.class, Schema.INT64_SCHEMA);
        fieldLookup.put(Float.class, Schema.FLOAT32_SCHEMA);
        fieldLookup.put(Double.class, Schema.FLOAT64_SCHEMA);
        fieldLookup.put(Boolean.class, Schema.BOOLEAN_SCHEMA);
        fieldLookup.put(ArrayList.class, SchemaBuilder.array((Schema)Schema.STRING_SCHEMA).build());
        fieldLookup.put(Date.class, Timestamp.SCHEMA);
        FIELD_LOOKUP = ImmutableMap.copyOf(fieldLookup);
    }
}
