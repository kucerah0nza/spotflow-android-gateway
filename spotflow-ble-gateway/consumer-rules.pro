# HiveMQ MQTT client relies on some optional dependencies resolved reflectively.
-dontwarn io.reactivex.rxjava3.**
-dontwarn org.jctools.**
-dontwarn io.netty.**
-keep class io.spotflow.ble.** { *; }
