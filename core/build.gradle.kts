plugins {
	`java-convention`
	`java-library`
}

dependencies {
	implementation(libs.spring.boot.starter)
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.validation)
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	
	testImplementation(libs.spring.boot.starter.test)
}
