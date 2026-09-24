# HiveMQ MQTT client (per its Android guide): Netty and JCTools access some members reflectively.
-keepclassmembernames class io.netty.** { *; }
-keepclassmembers class org.jctools.** { *; }
-dontwarn io.reactivex.rxjava3.**
-dontwarn org.jctools.**
-dontwarn io.netty.**

# The library itself uses no reflection; its manifest-declared service is kept by AAPT-generated rules.
