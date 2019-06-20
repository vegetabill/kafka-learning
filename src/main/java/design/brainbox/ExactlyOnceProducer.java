package design.brainbox;


import design.brainbox.util.SimpleJson;
import jdk.internal.util.EnvUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class ExactlyOnceProducer
{
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceProducer.class);
    public static final String TOPIC = "bank-account-transactions-v2";

    private static String randomCustomer() {
        List<String> customers = Arrays.asList("osito", "oscuro", "zorro");
        Collections.shuffle(customers);
        return customers.get(0);
    }

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private static String generateMessage(String customer) {
        Random r = new Random();

        Map<String, Object> data = new HashMap<>();
        Integer amount = r.nextInt(100) + 1;

        data.put("name", customer);
        data.put("amount", amount);
        data.put("time", DATE_FORMAT.format(new Date()));

        return SimpleJson.serialize(data);
    }

    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", EnvUtils.getEnvVar("BOOTSTRAP_SERVER"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
                    Thread.sleep(100);
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
