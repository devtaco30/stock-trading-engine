package com.flab.stocktradingengine.account.worker.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.account.worker.lifecycle.MatchingDestinationResolverLifecycle;
import com.flab.stocktradingengine.account.worker.lifecycle.MatchingRejectReplyReceiverLifecycle;
import com.flab.stocktradingengine.account.worker.messaging.AeronMatchingOrderSender;
import com.flab.stocktradingengine.account.worker.messaging.MatchingRejectReplyReceiver;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

/**
 * 매칭 샤딩 U9 — {@code matching-shard-routing.endpoints}(매칭 pod 주소 풀)가 있을 때만 켜지는 배선.
 * 주소마다 Publication을 미리 열고(아직 뜨지 않은 pod의 것은 연결 전 상태로 두고 오류로 보지 않는다),
 * 맵 토픽을 읽는 resolver를 만들어 {@link AeronMatchingOrderSender}에 함께 넘긴다. 설정이 없으면
 * {@link MatchingOrderSenderConfig}의 기존 Publication 1개 경로가 쓰인다.
 */
@Configuration
@ConditionalOnProperty(prefix = "matching-shard-routing", name = "endpoints[0]")
@EnableConfigurationProperties(MatchingShardRoutingProperties.class)
public class MatchingShardRoutingConfig {

    private static final Logger log = LoggerFactory.getLogger(MatchingShardRoutingConfig.class);

    @Bean
    public StockSlotHasher matchingStockSlotHasher(MatchingShardRoutingProperties properties) {
        return new StockSlotHasher(properties.stockSlotCount());
    }

    /** 이름을 고정해 두어 테스트가 브로커 없이 대역 컨슈머로 갈아 끼울 수 있다. 맵 토픽은 그룹 없이 assign으로 읽는다. */
    @Bean
    @ConditionalOnMissingBean(name = "matchingShardMapConsumer")
    public Consumer<Integer, String> matchingShardMapConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaConsumer<>(matchingShardMapConsumerProperties(bootstrapServers));
    }

    /**
     * 맵 토픽은 매칭 shard가 만든다. 이 컨슈머가 없는 토픽을 조회하다 1파티션짜리 빈 토픽을 자동 생성하면
     * 매칭 shard가 만들 토픽과 어긋나므로 자동 생성을 끈다.
     */
    static Properties matchingShardMapConsumerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return props;
    }

    @Bean(destroyMethod = "close")
    public MatchingDestinationResolver matchingDestinationResolver(
            Consumer<Integer, String> matchingShardMapConsumer,
            StockSlotHasher matchingStockSlotHasher,
            MatchingShardRoutingProperties properties) {
        log.info("[계좌] 매칭 담당 소스: matching-shard-map 구독 — 종목 slot {}개, 주소 풀 {}개",
            matchingStockSlotHasher.stockSlotCount(), properties.endpoints().size());
        return new MatchingDestinationResolver(matchingShardMapConsumer, matchingStockSlotHasher,
            Set.copyOf(properties.endpoints()));
    }

    @Bean
    public SmartLifecycle matchingDestinationResolverLifecycle(MatchingDestinationResolver matchingDestinationResolver) {
        return new MatchingDestinationResolverLifecycle(matchingDestinationResolver);
    }

    @Bean(destroyMethod = "close")
    public MatchingOrderPublications matchingOrderPublications(Aeron aeron, MatchingShardRoutingProperties properties) {
        Map<String, Publication> publicationsByEndpoint = new LinkedHashMap<>();
        for (String endpoint : properties.endpoints()) {
            publicationsByEndpoint.put(endpoint, aeron.addPublication(endpoint, AeronStreamIds.MATCHING_INTAKE));
        }
        return new MatchingOrderPublications(publicationsByEndpoint);
    }

    @Bean
    public AeronMatchingOrderSender matchingOrderSender(
            MatchingDestinationResolver matchingDestinationResolver,
            MatchingOrderPublications matchingOrderPublications,
            StockSlotHasher matchingStockSlotHasher,
            @Value("${matching-order.holding-capacity-per-slot:65536}") int holdingCapacityPerSlot) {
        return new AeronMatchingOrderSender(matchingDestinationResolver, matchingOrderPublications.byEndpoint(),
            matchingStockSlotHasher, holdingCapacityPerSlot);
    }

    /** 체결을 받는 주소에서 스트림 번호만 다르게 구독한다(새 포트 없음). */
    @Bean(destroyMethod = "close")
    public Subscription matchingRejectReplySubscription(
            Aeron aeron,
            @Value("${transport.fill.channel:" + AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return aeron.addSubscription(fillChannel, AeronStreamIds.REJECT_REPLY);
    }

    @Bean
    public MatchingRejectReplyReceiver matchingRejectReplyReceiver(
            Subscription matchingRejectReplySubscription, AeronMatchingOrderSender matchingOrderSender) {
        return new MatchingRejectReplyReceiver(matchingRejectReplySubscription, matchingOrderSender::acceptRejectedOrder);
    }

    @Bean
    public SmartLifecycle matchingRejectReplyReceiverLifecycle(MatchingRejectReplyReceiver matchingRejectReplyReceiver) {
        return new MatchingRejectReplyReceiverLifecycle(matchingRejectReplyReceiver);
    }

    /** 주소 풀의 주소마다 미리 열어 둔 Publication들. 종료할 때 모두 닫는다. */
    public record MatchingOrderPublications(Map<String, Publication> byEndpoint) {

        public void close() {
            for (Publication publication : byEndpoint.values()) {
                publication.close();
            }
        }
    }
}
