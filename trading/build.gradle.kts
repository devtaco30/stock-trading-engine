plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	implementation(project(":account"))
	implementation(project(":market"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.kafka)
	implementation(libs.spring.boot.starter.data.redis)

	runtimeOnly(libs.postgresql)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.kafka.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
