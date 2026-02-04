plugins {
	`java-convention`
	`java-library`
}

dependencies {
	// Jackson annotations only (compile-time)
	compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.17.0")
	
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
}
