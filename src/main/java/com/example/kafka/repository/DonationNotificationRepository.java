package com.example.kafka.repository;

import com.example.kafka.entity.DonationNotification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DonationNotificationRepository extends JpaRepository<DonationNotification, Long> {

    // 안 읽은 알림 조회
    List<DonationNotification> findByIsReadFalseOrderByCreatedAtDesc();

    // 전체 알림 조회 (최신순)
    List<DonationNotification> findAllByOrderByCreatedAtDesc();
}
