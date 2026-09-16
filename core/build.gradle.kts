plugins {
	`java-convention`
	`java-library`
}

dependencies {
	implementation(libs.snowflake)
	implementation(libs.agrona) // wire 패키지의 Aeron 바이너리 코덱(DirectBuffer/MutableDirectBuffer)이 씀
	implementation(libs.hdrhistogram) // time.LatencyHistogram의 백분위 계산 — public API에 안 드러나 implementation

	// Jackson annotations only (compile-time)
	compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.17.0")
	// Hibernate Generator/IdGeneratorType (compile-time; runtime from api/trading etc.)
	compileOnly("org.hibernate.orm:hibernate-core:6.4.1.Final")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Agrona는 JDK 17에서 내부 클래스 jdk.internal.misc.Unsafe와 sun.nio.ch에 접근한다.
// 모듈 시스템이 기본으로 막으므로 테스트 JVM에 개방 플래그를 준다(matching-disruptor·account-disruptor와 동일 이유).
tasks.withType<Test>().configureEach {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}
