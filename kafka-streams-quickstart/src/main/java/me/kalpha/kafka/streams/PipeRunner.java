package me.kalpha.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Component
public class PipeRunner implements ApplicationRunner {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker.deogi:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Topology 구성
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("streams-plaintext-input").to("streams-pipe-output");
        final Topology topology = builder.build();
        logger.info("Topology : " + topology.describe().toString());
        //Topology와 Properties를 가지고 Streams 생성
        final KafkaStreams streams = new KafkaStreams(topology, props);
        //메인 스레트와 새로운 스레드 간의 정보 전달을 위해 CountDownLatch 생성
        final CountDownLatch latch = new CountDownLatch(1);

        //Ctrl+C를 처리하기 위한 핸들러 추가
        Runtime.getRuntime().addShutdownHook(
                new Thread("streams-shutdown-hook") {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
