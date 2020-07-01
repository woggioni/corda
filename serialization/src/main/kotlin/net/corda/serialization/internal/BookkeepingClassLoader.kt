package net.corda.serialization.internal

import net.corda.core.serialization.SerializationToken
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SerializeAsTokenContext
import net.corda.serialization.internal.amqp.custom.ClassSerializer
import java.io.ByteArrayOutputStream
import java.io.File
import java.util.*
import kotlin.collections.HashSet


class BookkeepingClassLoader(parent: ClassLoader? = getSystemClassLoader(), classes : Iterable<Class<*>> = emptyList()) : ClassLoader(parent), SerializeAsToken {
    private val _classes = HashSet<Class<*>>().apply {
        addAll(classes)
    }

    val classes :Set<Class<*>>
    get() = Collections.unmodifiableSet(_classes)

    override fun loadClass(name: String, resolve : Boolean): Class<*> {
        return when {
            name.startsWith("java.") -> {
                parent.loadClass(name)
            }
            else -> {
                loadClassFromFile(name).also {
                    _classes.add(it)
                }
            }
        }
    }

    private fun loadClassFromFile(className: String): Class<*> {
        val classPath = className.replace('.', File.separatorChar) + ".class"
        return parent.getResourceAsStream(classPath)?.use  { inputStream ->
            val byteStream = ByteArrayOutputStream()
            val buffer = ByteArray(0x1000)
            while (true) {
                val read = inputStream.read(buffer)
                if (read == -1) break
                byteStream.write(buffer, 0, read)
            }
            byteStream.toByteArray().let {
                defineClass(className, it, 0, it.size)
            }
        } ?: throw ClassNotFoundException(className)
    }

    class Classes(classes : Iterable<Class<*>>) : SerializationToken, Iterable<String> by (classes.map {it.name}) {
        override fun fromToken(context: SerializeAsTokenContext) {
            val cl = javaClass.classLoader
            BookkeepingClassLoader(cl, map(cl::loadClass))
        }
    }

    override fun toToken(context: SerializeAsTokenContext): SerializationToken = Classes(classes)
}