package com.sohu.saccounts;

import com.google.common.base.Strings;
import com.sohu.sce.kafkaLogCollector.producer.KafkaProducer;
import com.sohu.sce.kafkaLogCollector.producer.internal.KafkaProducerImpl;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * User: guohaozhao (guohaozhao116008@sohu-inc.com)
 * Date: 4/1/14 15:51
 * kafka sink for flume-ng
 */
public class KafkaSink extends AbstractSink {

    private static final Logger logger = Logger.getLogger(KafkaSink.class);

    private static KafkaProducer<String, String> kafkaProducer = new KafkaProducerImpl<String, String>();

    private static String topic = "flume";

    static {
        topic = System.getProperty("topic");
        if (Strings.isNullOrEmpty(topic)) {
            throw new ConfigurationException("Kafka topic must be specified.");
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Status.READY;

            }
            kafkaProducer.send(topic, new String(event.getBody()));
            tx.commit();
            return Status.READY;
        } catch (Exception e) {
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch (Exception e2) {
                logger.error("Rollback Exception:{}", e2);
            }
            logger.error("KafkaSink Exception:{}", e);
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }
}
