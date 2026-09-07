plugins {
	`java-convention`
	`java-library`
}

dependencies {
	implementation(project(":core"))     // OrderSide
	implementation(project(":trading"))  // OrderBook, OrderEntry, FillResult

	implementation(libs.disruptor)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
