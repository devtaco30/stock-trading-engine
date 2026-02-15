plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	implementation(project(":user"))
	implementation(project(":market"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.data.jpa)
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.bootJar {
	enabled = false
}
