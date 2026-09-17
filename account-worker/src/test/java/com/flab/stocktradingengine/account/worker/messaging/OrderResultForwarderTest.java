package com.flab.stocktradingengine.account.worker.messaging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.flab.stocktradingengine.account.worker.recovery.OrderResultForwardPositionStore;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;
import com.flab.stocktradingengine.kafka.KafkaTopics;

/**
 * U3-a — 디코딩된 결과 엔트리가 실제 {@code order-results} 토픽에 도착하는지(임베디드 Kafka),
 * 손상 프레임은 예외 없이 skip하고 아무것도 발행하지 않는지 검증한다. Aeron Subscription은 이
 * 클래스의 폴 루프에만 쓰이므로(디코딩·발행 로직과 무관) {@code AccountFillReceiverTest}(account-disruptor)와
 * 같은 방식으로 {@code onFragment}를 직접 호출한다.
 */
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = "order-results")
class OrderResultForwarderTest {

    private static final OrderResultCodec CODEC = new OrderResultCodec();
    private static final long DUMMY_RECORDING_ID = 1L;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaTemplate<String, Object> realKafkaTemplate;

    @AfterEach
    void tearDown() {
        if (realKafkaTemplate != null) {
            realKafkaTemplate.destroy();
        }
    }

    @Test
    void 디코딩된_엔트리가_실제_토픽에_도착한다() {
        realKafkaTemplate = newRealKafkaTemplate();
        OrderResultForwarder forwarder =
            new OrderResultForwarder(null, realKafkaTemplate, mock(OrderResultForwardPositionStore.class), DUMMY_RECORDING_ID);
        OrderResultEntry entry = new OrderResultEntry(1L, 100L, "r1", OrderVerdict.ACCEPTED, null, 1_234L);

        forwarder.onFragment(encode(entry), 0, encodedLength(entry), null);

        Consumer<String, String> consumer = newStringConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, KafkaTopics.orderResults());
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, KafkaTopics.orderResults(), Duration.ofSeconds(5));
        consumer.close();

        assertEquals("1", record.key());
        assertTrue(record.value().contains("\"accountId\":1"));
        assertTrue(record.value().contains("\"orderId\":100"));
        assertTrue(record.value().contains("\"requestId\":\"r1\""));
        assertTrue(record.value().contains("\"verdict\":\"ACCEPTED\""));
    }

    @Test
    void 손상된_프레임은_예외없이_skip하고_아무것도_발행하지_않는다() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> mockKafkaTemplate = mock(KafkaTemplate.class);
        OrderResultForwarder forwarder =
            new OrderResultForwarder(null, mockKafkaTemplate, mock(OrderResultForwardPositionStore.class), DUMMY_RECORDING_ID);

        UnsafeBuffer poison = new UnsafeBuffer(new byte[2]); // OrderResultCodec 최소 고정 길이(18)에 한참 못 미침

        assertDoesNotThrow(() -> forwarder.onFragment(poison, 0, poison.capacity(), null));
        verifyNoInteractions(mockKafkaTemplate);
    }

    @Test
    void 정상_프레임은_토픽_키_이벤트를_정확히_실어_send를_부른다() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> mockKafkaTemplate = mock(KafkaTemplate.class);
        OrderResultForwarder forwarder =
            new OrderResultForwarder(null, mockKafkaTemplate, mock(OrderResultForwardPositionStore.class), DUMMY_RECORDING_ID);
        OrderResultEntry entry = new OrderResultEntry(7L, 0L, "r7", OrderVerdict.REJECTED, "INSUFFICIENT", 7_000L);

        forwarder.onFragment(encode(entry), 0, encodedLength(entry), null);

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mockKafkaTemplate).send(eq(KafkaTopics.orderResults()), eq("7"), eventCaptor.capture());
        assertEquals(
            "OrderResultEvent[accountId=7, orderId=0, requestId=r7, verdict=REJECTED, rejectReason=INSUFFICIENT, epochMillis=7000]",
            eventCaptor.getValue().toString());
    }

    private UnsafeBuffer encode(OrderResultEntry entry) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        CODEC.encode(buffer, 0, entry);
        return buffer;
    }

    private int encodedLength(OrderResultEntry entry) {
        UnsafeBuffer scratch = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        return CODEC.encode(scratch, 0, entry);
    }

    private KafkaTemplate<String, Object> newRealKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(producerFactory);
    }

    private Consumer<String, String> newStringConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("order-result-forwarder-test", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
    }

    @Configuration
    static class EmptyConfig {
    }
}
