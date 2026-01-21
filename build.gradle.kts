plugins {
	java
	alias(libs.plugins.spring.boot) apply false
	alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
	group = "com.flab"
	version = "0.0.1-SNAPSHOT"
}

subprojects {
	apply(plugin = "java")
	apply(plugin = "io.spring.dependency-management")
	
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
	
	dependencyManagement {
		imports {
			mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}")
		}
	}
	
	tasks.withType<Test> {
		useJUnitPlatform()
	}
}
