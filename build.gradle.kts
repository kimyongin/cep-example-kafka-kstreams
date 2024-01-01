plugins {
    id("org.springframework.boot") version "3.2.1"
    id("io.spring.dependency-management") version "1.1.4"
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation ("org.springframework.boot:spring-boot-starter")
    implementation ("org.springframework.boot:spring-boot-starter-web")
    implementation ("org.springframework.boot:spring-boot-starter-logging")

    implementation ("org.springframework.kafka:spring-kafka:3.1.1")
    implementation("org.apache.kafka:kafka-streams:3.6.1")
    implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka-streams:4.1.0")

    implementation ("ch.qos.logback.contrib:logback-json-classic:0.1.5")
    implementation ("ch.qos.logback.contrib:logback-jackson:0.1.5")
    implementation ("com.fasterxml.jackson.core:jackson-databind:2.16.1")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")

    compileOnly ("org.projectlombok:lombok:1.18.30")
    annotationProcessor ("org.projectlombok:lombok:1.18.30")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}