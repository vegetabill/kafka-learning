package design.brainbox;


import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class ExactlyOnceProducer
{
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceProducer.class);
    public static final String TOPIC = "bank-account-transactions-v4";

    private static String randomCustomer() {
        List<String> customers = Arrays.asList("osito", "oscuro", "zorro");
        Collections.shuffle(customers);
        return customers.get(0);
    }

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private static String generateMessage(String customer) {
        Integer amount = ThreadLocalRandom.current().nextInt(100) + 1;
        ObjectNode record = JsonNodeFactory.instance.objectNode();
        record.put("name", customer);
        record.put("amount", amount);
        record.put("time", new Date().getTime());
        return record.toString();
    }

    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {

            while(true) {
                String customer = randomCustomer();
                String key = customer;
                String value = generateMessage(customer);

                logger.info("sending: " + value);
                ProducerRecord<String, String> message = new ProducerRecord<>(TOPIC, key, value);
                producer.send(message);
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }


        } finally {
            producer.close();
        }


    }
}
