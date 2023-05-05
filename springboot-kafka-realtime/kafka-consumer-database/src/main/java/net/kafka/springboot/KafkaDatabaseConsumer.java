package net.kafka.springboot;

import net.kafka.springboot.entity.WikimediaData;
import net.kafka.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    private WikimediaDataRepository rep;

    public KafkaDatabaseConsumer(WikimediaDataRepository rep) {
        this.rep = rep;
    }

    @KafkaListener(topics = "wikimedia_recent", groupId = "myRealGroup")
    public void consume(String eventMessage) {
        LOGGER.info(String.format("Event Message received -> %s", eventMessage));

        WikimediaData data = new WikimediaData();
        data.setWikiEventData("eventMessage");

        rep.save(data);
    }
}
