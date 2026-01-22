plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))
	implementation(project(":auth"))
	implementation(project(":account"))
	implementation(project(":trading"))
	implementation(project(":settlement"))
	implementation(project(":market"))
	
	implementation(libs.spring.boot.starter.web)
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.validation)
	
	// PostgreSQL
	runtimeOnly(libs.postgresql)
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
}
