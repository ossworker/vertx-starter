# Build the fat jar first:  mvn -pl example/zenoh-cluster-spring-boot-example -am package
# Then build & push the image:
#   docker build -t your-registry/zenoh-cluster-spring-boot-example:1.0.0-SNAPSHOT \
#     example/zenoh-cluster-spring-boot-example
#
# CRITICAL: the jar must contain the LINUX native library
# (linux-amd64/libzenoh_ffi_jni.so and/or linux-aarch64/...), NOT the macOS
# darwin-arm64 dylib that the local build currently bundles. Rebuild the
# boltffi-zenoh native for linux and copy it under
# zenoh-jni/src/main/resources/linux-amd64/ before packaging, or the pod will
# fail at zenoh session open with an UnsatisfiedLinkError on Linux.
FROM eclipse-temurin:21-jre

WORKDIR /app
COPY target/zenoh-cluster-spring-boot-example-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
