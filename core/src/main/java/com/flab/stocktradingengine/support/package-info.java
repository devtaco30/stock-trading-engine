/**
 * Snowflake 스타일 64비트 ID 생성 지원.
 *
 * <h2>클래스 요약</h2>
 * <ul>
 *   <li><b>SnowflakeIdGenerator</b> – 실제 ID 발급. com.relops:snowflake 위임. nodeId(0~1023)로 생성 후 {@code nextId()} 호출 시 64비트 long 반환.</li>
 *   <li><b>SnowflakeIdGeneratorHolder</b> – 정적 홀더. Hibernate Generator가 Spring 빈을 직접 주입받지 못하므로, 기동 시 {@link SnowflakeIdGeneratorHolder#set}으로 등록해 두고 {@link SnowflakeIdGeneratorHolder#get}으로 조회.</li>
 *   <li><b>SnowflakeGenerator</b> – Hibernate {@link org.hibernate.generator.BeforeExecutionGenerator}. INSERT 시 홀더에서 생성기를 꺼내 {@code nextId()} 호출해 엔티티 필드에 넣음.</li>
 *   <li><b>SnowflakeId</b> – 필드/메서드용 어노테이션. {@link SnowflakeGenerator}와 연결되어 있어, 이 어노테이션이 붙은 필드는 INSERT 시 자동으로 Snowflake ID가 채워짐.</li>
 * </ul>
 *
 * <h2>ID 값 구조 (Twitter 스타일)</h2>
 * 64비트 long: 1bit(0) + 41bit(ms 타임스탬프) + 10bit(노드 ID) + 12bit(동일 ms 내 시퀀스). 시간순 정렬·분산 환경에서 노드별 고유 보장.
 */
package com.flab.stocktradingengine.support;
