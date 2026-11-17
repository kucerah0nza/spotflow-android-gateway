# Rules every app that uses the library inherits. CI builds the demo's `minified` variant with R8 to
# check that they are enough for a shrunk build to go through.

# HiveMQ MQTT client (per its Android guide): Netty and JCTools access some members reflectively.
-keepclassmembernames class io.netty.** { *; }
-keepclassmembers class org.jctools.** { *; }
-dontwarn io.reactivex.rxjava3.**
-dontwarn org.jctools.**
-dontwarn io.netty.**

# Optional integrations of the MQTT client and Netty that are not on an Android classpath: SLF4J (the
# client logs through it only when it is present) and BlockHound (a JVM debugging agent).
-dontwarn org.slf4j.**
-dontwarn reactor.blockhound.**

# The library itself uses no reflection; its manifest-declared service is kept by AAPT-generated rules.
