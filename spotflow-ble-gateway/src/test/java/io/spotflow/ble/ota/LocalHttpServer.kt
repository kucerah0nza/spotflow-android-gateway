package io.spotflow.ble.ota

import java.io.Closeable
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.Collections
import kotlin.concurrent.thread

/**
 * A minimal HTTP/1.1 server for tests: one request per connection (`Connection: close`), routed by path.
 * (The JDK's own server is not on the Android unit-test classpath.)
 */
internal class LocalHttpServer : Closeable {

    class Request(val path: String, val headers: Map<String, String>) {
        fun header(name: String): String? = headers[name.lowercase()]
    }

    /** Writes the response; [cut] sends [Response.body]'s first bytes only and then drops the connection. */
    class Response(
        val code: Int,
        val body: ByteArray = ByteArray(0),
        val headers: Map<String, String> = emptyMap(),
        /** Content-Length is left out (the body ends with the connection). */
        val undeclaredLength: Boolean = false,
        val cut: Int? = null,
    )

    private val socket = ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))
    val port: Int get() = socket.localPort
    private val routes = Collections.synchronizedMap(HashMap<String, (Request) -> Response>())

    /** Every request served, in order. */
    val requests: MutableList<Request> = Collections.synchronizedList(ArrayList())

    init {
        thread(isDaemon = true, name = "local-http") {
            while (!socket.isClosed) {
                val client = runCatching { socket.accept() }.getOrNull() ?: break
                thread(isDaemon = true) { client.use { serve(it) } }
            }
        }
    }

    fun route(path: String, handler: (Request) -> Response) {
        routes[path] = handler
    }

    private fun serve(client: Socket) {
        val input = client.getInputStream().bufferedReader(Charsets.ISO_8859_1)
        val requestLine = input.readLine() ?: return
        val headers = HashMap<String, String>()
        while (true) {
            val line = input.readLine() ?: return
            if (line.isEmpty()) break
            val colon = line.indexOf(':')
            headers[line.substring(0, colon).trim().lowercase()] = line.substring(colon + 1).trim()
        }
        val request = Request(requestLine.split(" ")[1], headers)
        requests += request
        val response = routes[request.path]?.invoke(request) ?: Response(404)
        write(client.getOutputStream(), response)
    }

    private fun write(out: OutputStream, response: Response) {
        val head = StringBuilder("HTTP/1.1 ${response.code} X\r\nConnection: close\r\n")
        response.headers.forEach { (name, value) -> head.append("$name: $value\r\n") }
        if (!response.undeclaredLength) head.append("Content-Length: ${response.body.size}\r\n")
        head.append("\r\n")
        out.write(head.toString().toByteArray(Charsets.ISO_8859_1))
        out.write(response.body, 0, response.cut ?: response.body.size)
        out.flush()
    }

    override fun close() = socket.close()
}
