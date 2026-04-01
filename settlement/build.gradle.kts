plugins {
	`java-convention`
	`java-library`
}

dependencies {
	implementation(project(":core"))
	implementation(project(":account"))
	implementation(project(":trading"))

	implementation(libs.spring.boot.starter.data.jpa)

	runtimeOnly(libs.postgresql)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
