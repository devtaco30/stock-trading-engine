plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	implementation(project(":trading"))
	implementation(project(":account"))

	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.kafka)
	implementation("com.fasterxml.jackson.core:jackson-databind") // 접수 지연 측정 결과(JSON) 파일 출력용(버전은 Spring Boot BOM)

	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.kafka.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
