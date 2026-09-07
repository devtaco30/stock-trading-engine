plugins {
	`java-convention`
	`java-library`
}

// JMH 벤치마크 전용 소스셋. 서드파티 JMH Gradle 플러그인(Gradle 9 호환 미보장)에 기대지 않고
// 소스셋을 직접 만들어, jmh 코드가 main 산출물과 프로젝트 의존성을 그대로 쓰게 배선한다.
val jmh: SourceSet by sourceSets.creating

// jmh 소스셋의 의존성 구성이 main 의 implementation/runtimeOnly 를 상속하게 한다.
configurations["jmhImplementation"].extendsFrom(configurations.implementation.get())
configurations["jmhRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

dependencies {
	implementation(project(":core"))     // OrderSide
	implementation(project(":trading"))  // OrderBook, OrderEntry, FillResult

	implementation(libs.disruptor)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

	// JMH 코어(런타임)와 벤치 클래스 생성용 애노테이션 프로세서
	"jmhImplementation"(libs.jmh.core)
	"jmhAnnotationProcessor"(libs.jmh.generator.annprocess)
}

// jmh 소스셋이 main 의 컴파일 산출물(MatchingEngine 등)을 참조하도록 classpath 에 더한다.
sourceSets.named("jmh") {
	compileClasspath += sourceSets.main.get().output
	runtimeClasspath += sourceSets.main.get().output
}

// 실행: ./gradlew :matching-disruptor:jmh --args="-wi 1 -i 1 -f 1"
tasks.register<JavaExec>("jmh") {
	group = "benchmark"
	description = "JMH 매칭 처리량 벤치마크 실행"
	mainClass.set("org.openjdk.jmh.Main")
	classpath = sourceSets.named("jmh").get().runtimeClasspath
}
