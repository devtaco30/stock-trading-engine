plugins {
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.data.jpa)
	
	// Auth0 JWT 
	implementation(libs.auth0.java.jwt)
	implementation(libs.auth0.jwks.rsa)
	
	// JWT (필요시 사용)
	implementation(libs.bundles.jwt)
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
}
