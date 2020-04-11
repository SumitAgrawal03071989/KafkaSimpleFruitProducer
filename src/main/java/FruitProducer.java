import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class FruitProducer {
    public static void main(String[] args){

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "0:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

        System.out.println("******** Starting producer ********");

        try {
            while(true) {
                String randomFruit = getRandomFruit() ;
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("topicFruitBasket", randomFruit);
                producer.send(record);
                System.out.println("Delivered " + randomFruit);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static String getRandomFruit() {
        String fruit = null;
        Random rand = new Random();
        switch(rand.nextInt(3)){
            case 0:
                fruit = "Mango";
                break;

            case 1:
                fruit = "Banana";
                break;

            case 2:
                fruit = "Apple";
                break;

            default:
                fruit = "Orange";
                break;
        }
        return fruit;
    }

}
