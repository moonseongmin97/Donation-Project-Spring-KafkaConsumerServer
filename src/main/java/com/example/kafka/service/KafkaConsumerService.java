package com.example.kafka.service;

import com.example.kafka.entity.DonationNotification;
import com.example.kafka.repository.DonationNotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final DonationNotificationRepository notificationRepository;
    private final RestTemplate restTemplate;

    @Value("${chat.server.url}")
    private String chatServerUrl;

    @KafkaListener(topics = "donation-topic", groupId = "donation-consumer-group")
    public void consumeDonation(ConsumerRecord<String, Map<String, Object>> record, Acknowledgment ack) {
        try {
            log.info("=== 기부 메시지 수신 ===");
            log.info("Offset: {}, Key: {}", record.offset(), record.key());
            log.info("Value: {}", record.value());

            Map<String, Object> donationData = record.value();

            // 1. DB에 알림 저장
            DonationNotification notification = saveNotification(donationData);
            log.info("알림 DB 저장 완료 - id: {}", notification.getId());

            // 2. Chat 서버에 브로드캐스트 요청
            sendBroadcastToChat(donationData);
            log.info("채팅 서버 브로드캐스트 완료");

            // 수동 커밋
            ack.acknowledge();
            log.info("메시지 처리 완료 및 커밋");

        } catch (Exception e) {
            log.error("기부 메시지 처리 실패", e);
        }
    }

    /**
     * DB에 기부 알림 저장
     */
    private DonationNotification saveNotification(Map<String, Object> donationData) {
        Long userId = donationData.get("userId") != null ?
                ((Number) donationData.get("userId")).longValue() : null;
        String userName = (String) donationData.get("userName");

        BigDecimal amount = BigDecimal.ZERO;
        if (donationData.get("amount") != null) {
            amount = new BigDecimal(donationData.get("amount").toString());
        }

        String message = String.format("%s님이 %s원을 기부했습니다!", userName, amount.toString());

        DonationNotification notification = DonationNotification.builder()
                .userId(userId)
                .userName(userName)
                .amount(amount)
                .message(message)
                .build();

        return notificationRepository.save(notification);
    }

    /**
     * Chat 서버에 전체 브로드캐스트 요청
     */
    private void sendBroadcastToChat(Map<String, Object> donationData) {
        try {
            String userName = (String) donationData.get("userName");
            Object amount = donationData.get("amount");

            // 프론트엔드 형식에 맞춤: { user: "xxx", msg: "xxx" }
            Map<String, Object> broadcastMessage = new HashMap<>();
            broadcastMessage.put("user", "🎁 기부알림");
            broadcastMessage.put("msg", String.format("%s님이 %s원을 기부했습니다!", userName, amount));
            broadcastMessage.put("type", "DONATION_ALERT");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(broadcastMessage, headers);

            String url = chatServerUrl + "/api/broadcast";
            restTemplate.postForEntity(url, request, String.class);

            log.info("Chat 서버 브로드캐스트 성공 - {}", broadcastMessage);
        } catch (Exception e) {
            log.error("Chat 서버 브로드캐스트 실패", e);
            // 브로드캐스트 실패해도 전체 트랜잭션은 계속 진행
        }
    }
}
