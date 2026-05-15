package com.virinchi.apiservice.service;

import com.virinchi.apiservice.entity.OutboxEvent;
import com.virinchi.apiservice.entity.OutboxStatus;
import com.virinchi.apiservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherService {

    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events =
                outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (events.isEmpty()) {
            return;
        }

        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(
                        event.getTopic(),
                        event.getMessageKey(),
                        event.getPayload()
                ).get();

                event.setStatus(OutboxStatus.SENT);
                event.setSentAt(Instant.now());
                event.setLastError(null);

                outboxEventRepository.save(event);

            } catch (Exception e) {
                int nextRetryCount = event.getRetryCount() + 1;

                event.setRetryCount(nextRetryCount);
                event.setLastError(e.getMessage());

                if (nextRetryCount >= MAX_RETRIES) {
                    event.setStatus(OutboxStatus.FAILED);
                    log.error("Outbox event permanently failed: {}", event.getId(), e);
                } else {
                    event.setStatus(OutboxStatus.PENDING);
                    log.warn("Outbox event publish failed, will retry: {}", event.getId(), e);
                }

                outboxEventRepository.save(event);
            }
        }
    }
}