// v2 계좌 상태 프로젝션 워커. account-worker가 Kafka account-state로 발행한 full-state를
// 소비해 v2 전용 read model 테이블(account_projection)에 stale-guard upsert 한다(계좌 상태
// 영속/프로젝션 트랙 U3). settlement-worker와 같은 결(DB를 가진 v2 워커) — 다만 이 워커의
// 테이블은 read model(재구성 가능)이라 내구성 경계가 아니므로, settlement-worker와 달리
// 테스트는 H2를 쓴다(운영은 application-local.yml의 Postgres).
plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core")) // AccountStateEvent, KafkaTopics

	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.kafka)
	implementation("com.fasterxml.jackson.core:jackson-databind") // Spring Kafka JsonSerializer/Deserializer 런타임 필요(버전은 Spring Boot BOM)

	// PostgreSQL (운영, application-local.yml), H2 (기본 — 테스트)
	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.kafka.test) // @EmbeddedKafka — 실 로컬 Kafka 대신 임베디드로 발행→소비 e2e
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("com.h2database:h2")
}
