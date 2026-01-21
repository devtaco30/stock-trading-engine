rootProject.name = "stock-trading-engine"

pluginManagement {
	repositories {
		gradlePluginPortal()
		mavenCentral()
	}
	plugins {
		id("org.springframework.boot") version "3.2.0" apply false
		id("io.spring.dependency-management") version "1.1.4" apply false
	}
}

dependencyResolutionManagement {
	repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
	repositories {
		mavenCentral()
	}
}

include("core")
include("auth")
include("account")
include("trading")
include("settlement")
include("market")
include("api")
