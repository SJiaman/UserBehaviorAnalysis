package fun.shijin.hotitems_analysis.util;

import org.apache.flink.table.descriptors.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author Jiaman
 * @Date 2021/4/11 19:12
 * @Desc
 */

public class KafkaProducerUtil {
    public static void main(String[] args) throws IOException {
        writeToKafka("hotitems");
    }

    private static void writeToKafka(String topic) throws IOException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop111:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        BufferedReader bufferedReader = new BufferedReader(new FileReader("F:\\Demo\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();
    }
}
