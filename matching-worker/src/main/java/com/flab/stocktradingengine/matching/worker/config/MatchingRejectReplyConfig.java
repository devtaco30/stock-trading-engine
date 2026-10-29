package com.flab.stocktradingengine.matching.worker.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.worker.messaging.RejectReplyPublisher;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;

import lombok.extern.slf4j.Slf4j;

/**
 * 담당이 아닌 주문의 거부 회신 경로 배선. 체결 fan-out과 같은 계좌 shard 주소 풀({@link MatchingFillPublications}가
 * 연 주소)마다 거부 회신 스트림({@link AeronStreamIds#REJECT_REPLY})의 Publication을 하나씩 더 열고, 새 포트는
 * 만들지 않는다. 아직 뜨지 않은 계좌 shard의 Publication은 연결 전 상태로 남아도 오류가 아니다. Archive 녹화는 걸지 않는다.
 */
@Slf4j
@Configuration
public class MatchingRejectReplyConfig {

    @Bean(destroyMethod = "close")
    public RejectReplyPublications rejectReplyPublications(Aeron aeron, MatchingFillPublications matchingFillPublications) {
        Map<String, ExclusivePublication> byEndpoint = new LinkedHashMap<>();
        for (String endpoint : matchingFillPublications.byEndpoint().keySet()) {
            byEndpoint.put(endpoint, aeron.addExclusivePublication(endpoint, AeronStreamIds.REJECT_REPLY));
        }
        log.info("[매칭] 거부 회신 스트림({}) Publication {}건 연결: {}", AeronStreamIds.REJECT_REPLY, byEndpoint.size(), byEndpoint.keySet());
        return new RejectReplyPublications(byEndpoint);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public RejectReplyPublisher rejectReplyPublisher(
            AccountDestinationResolver accountDestinationResolver, RejectReplyPublications rejectReplyPublications) {
        return new RejectReplyPublisher(accountDestinationResolver, rejectReplyPublications.byEndpoint());
    }
}
