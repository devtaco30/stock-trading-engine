plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
	jacoco
}

dependencies {
	implementation(project(":core"))
	implementation(project(":user"))
	implementation(project(":account"))
	implementation(project(":trading"))
	implementation(project(":settlement"))
	implementation(project(":market"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.validation)
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.data.redis)
	implementation(libs.spring.kafka)
	implementation(libs.slf4j.api)
	implementation(libs.spring.kafka)

	// Aeron: v2 게이트웨이 → 계좌 인테이크 발신 배선(fork5 U1b, account-worker/matching-worker 미러).
	// Publication·Aeron·MediaDriver 타입이 이 모듈 코드에 직접 등장해 implementation 재선언이 필요하다.
	// Archive는 안 쓴다 — 발신만 하는 클라이언트라 녹화 대상이 아니다.
	implementation(libs.aeron.driver)
	implementation(libs.aeron.client)

	// PostgreSQL (운영), H2 (로컬 bootRun 기본)
	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.kafka.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("com.h2database:h2")
}

// Aeron/Agrona는 JDK 17에서 내부 클래스 jdk.internal.misc.Unsafe와 sun.nio.ch에 접근한다.
// 모듈 시스템이 기본으로 막으므로 테스트 JVM에 개방 플래그를 준다(matching-worker와 동일 이유).
tasks.withType<Test>().configureEach {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}

// fork1, Unit 3a — bootRun은 별도 JVM으로 뜨는 실행 태스크라 위 test 개방 플래그가 안 미러된다.
// v2 게이트웨이(AccountOrderPublishConfig)가 Aeron MediaDriver를 띄우므로 같은 플래그가 필요하다.
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}

// 단위 테스트만 실행 (Mock 사용, Spring/H2 미사용). 서비스 패키지의 *Test만 포함.
tasks.register<Test>("unitTest") {
	group = "verification"
	description = "Runs only unit tests (service package, no Spring context)."
	useJUnitPlatform()
	include("**/service/*Test.class")
	testClassesDirs = sourceSets.test.get().output.classesDirs
	classpath = sourceSets.test.get().runtimeClasspath
}

tasks.jacocoTestReport {
	reports {
		xml.required.set(true)
		html.required.set(true)
	}
}
