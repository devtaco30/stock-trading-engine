plugins {
	`java-convention`
	`java-library`
}

dependencies {
	implementation(libs.snowflake)

	// Jackson annotations only (compile-time)
	compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.17.0")
	// Hibernate Generator/IdGeneratorType (compile-time; runtime from api/trading etc.)
	compileOnly("org.hibernate.orm:hibernate-core:6.4.1.Final")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
