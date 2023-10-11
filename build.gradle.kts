plugins {
    id("java")
    id("me.champeau.jmh") version "0.7.1"
    id("com.diffplug.spotless") version "6.22.0"
}

group = "org.opensearch"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // https://mvnrepository.com/artifact/org.openjdk.jmh/jmh-core
    jmh("org.openjdk.jmh:jmh-core:1.37")
    // https://mvnrepository.com/artifact/org.openjdk.jmh/jmh-generator-annprocess
    jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

tasks.compileJava {
    options.compilerArgs.addAll(listOf("--add-modules", "jdk.incubator.vector"))
}

tasks.compileJmhJava {
    options.compilerArgs.addAll(listOf("--add-modules", "jdk.incubator.vector"))
}

tasks.jmhRunBytecodeGenerator {
    jvmArgs.addAll("--add-modules", "jdk.incubator.vector")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-modules", "jdk.incubator.vector")
}

java {
    targetCompatibility = JavaVersion.VERSION_21
    sourceCompatibility = JavaVersion.VERSION_21
}

jmh {
    version = "1.37"
    jvmArgsAppend.addAll(listOf("--add-modules", "jdk.incubator.vector"))

    // Uncomment to enable profiling with perf.
    // profilers.add("perfnorm")
}

spotless {
    java {
        googleJavaFormat().aosp()
    }
}
