plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.data.jpa)
	
	// Auth0 JWT 
	implementation(libs.auth0.java.jwt)
	implementation(libs.auth0.jwks.rsa)
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
}

tasks.bootJar {
	enabled = false
}
