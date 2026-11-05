plugins {
	java
}

group = "com.flab"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(17))
	}
}

configurations {
	compileOnly {
		extendsFrom(annotationProcessor.get())
	}
}

dependencies {
	implementation(platform("org.springframework.boot:spring-boot-dependencies:3.4.0"))
}

tasks.withType<Test> {
	useJUnitPlatform {
		if (project.hasProperty("evidence")) {
			includeTags("evidence")
		} else {
			excludeTags("evidence")
		}
	}
	testLogging {
		events("passed", "skipped", "failed")
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
		showStandardStreams = true
	}
}
