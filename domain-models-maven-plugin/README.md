For documentation see main [README.md](../README.md).

Use `domain-models-maven-plugin` as a plugin in the `<plugins>` pom.xml section only. Please do not add it to the `<dependencies>` section, it is based on old libraries with security vulnerabilities. The vulnerabilities do not affect us when running `domain-models-maven-plugin` as a plugin but they may affect a module if those old dependencies are included as explicit dependencies.
