plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	implementation(project(":trading"))

	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.kafka)
	implementation(libs.spring.boot.starter.data.redis)
	implementation("com.fasterxml.jackson.core:jackson-databind")

	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.kafka.test)
	testImplementation(project(":settlement"))
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("com.h2database:h2")
}
