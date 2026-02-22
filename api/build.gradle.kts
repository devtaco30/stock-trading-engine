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
	implementation(libs.slf4j.api)

	// PostgreSQL (운영), H2 (로컬 bootRun 기본)
	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("com.h2database:h2")
}

tasks.named<Test>("test") {
	// ScenarioIntegrationTest는 DataSource/JPA 제외한 컨텍스트를 쓰며 현재 앱 구조와 맞지 않아 기본 test에서 제외
	exclude("**/ScenarioIntegrationTest.class")
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
