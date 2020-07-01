package net.corda.flow.necromancer

import com.sun.tools.attach.VirtualMachine
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.CheckpointSerializationDefaults
import net.corda.core.serialization.internal.SerializationEnvironment
import net.corda.core.serialization.internal.checkpointDeserialize
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.contextLogger
import net.corda.nodeapi.internal.lifecycle.NodeServicesContext
import net.corda.nodeapi.internal.rpc.client.AMQPClientSerializationScheme
import net.corda.nodeapi.internal.serialization.amqp.AMQPServerSerializationScheme
import net.corda.nodeapi.internal.serialization.kryo.KRYO_CHECKPOINT_CONTEXT
import net.corda.nodeapi.internal.serialization.kryo.KryoCheckpointSerializer
import net.corda.serialization.internal.AMQP_P2P_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_CLIENT_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_SERVER_CONTEXT
import net.corda.serialization.internal.AMQP_STORAGE_CONTEXT
import net.corda.serialization.internal.CheckpointSerializeAsTokenContextImpl
import net.corda.serialization.internal.SerializationFactoryImpl
import net.corda.serialization.internal.withTokenContext
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.lang.ClassLoader.getSystemClassLoader
import java.lang.management.ManagementFactory
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.net.URL
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipInputStream
import kotlin.streams.asSequence

inline fun <reified T> newProxyObject(crossinline methodInvocation : (proxy: Any?, method: Method, args: Array<Any?>?) -> Any) =
        Proxy.newProxyInstance(T::class.javaClass.classLoader, arrayOf<Class<*>>(T::class.java)) {
            proxy, method, args -> methodInvocation(proxy, method, args)
        } as T

class FlowLoader(archivePath: Path, parent: ClassLoader? = getSystemClassLoader()) : AutoCloseable {
    private val temporaryDir: Path
    private val checkpointSerializationContext : CheckpointSerializationContext

    val classLoader: ClassLoader

    companion object {

        private val log = contextLogger()
        private var quasarAgentRunning = false

        private val quasarAgentArgs = """x(antlr**;bftsmart**;co.pa
            ralleluniverse**;com.codahale**;com.esotericsoftware**;com.fasterxml*
            *;com.google**;com.ibm**;com.intellij**;com.jcabi**;com.nhaarman**;co
            m.opengamma**;com.typesafe**;com.zaxxer**;de.javakaffee**;groovy**;gr
            oovyjarjarantlr**;groovyjarjarasm**;io.atomix**;io.github**;io.netty*
            *;jdk**;kotlin**;net.corda.djvm**;djvm**;net.bytebuddy**;net.i2p**;or
            g.apache**;org.bouncycastle**;org.codehaus**;org.crsh**;org.dom4j**;o
            rg.fusesource**;org.h2**;org.hibernate**;org.jboss**;org.jcp**;org.jo
            da**;org.objectweb**;org.objenesis**;org.slf4j**;org.w3c**;org.xml**;
            org.yaml**;reflectasm**;rx**;org.jolokia**;com.lmax**;picocli**;liqui
            base**;com.github.benmanes**;org.json**;org.postgresql**;nonapi.io.gi
            thub.classgraph**)l(net.corda.djvm.**;net.corda.core.serialization.in
            ternal.**)
        """.replace("\n", "").replace(" ", "")

        private fun startQuasarAgent(archivePath: Path, temporaryDir: Path) {
            synchronized(this) {
                if (!quasarAgentRunning) {
                    val quasarJar = Files.list(temporaryDir.resolve("lib")).asSequence().firstOrNull {
                        it.fileName.toString().startsWith("quasar-core")
                    } ?: throw RuntimeException("Quasar jar not found in $archivePath")
                    val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().name
                    val pid = nameOfRunningVM.substring(0, nameOfRunningVM.indexOf('@'))
                    val vm = VirtualMachine.attach(pid)
                    vm.loadAgent(quasarJar.toString(), quasarAgentArgs)
                    vm.detach()
                    quasarAgentRunning = true
                }
            }
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
            val arguments = runtimeMxBean.inputArguments
            println(arguments)

            val p = Paths.get("/home/r3/checkpoints_debug.zip")
            val flowLoader = FlowLoader(p, null)
            val fiberFile = flowLoader.fibers().first()
            val fib = flowLoader.fiber(fiberFile)
            val cls2 = fib.javaClass
            println(cls2)
        }
    }

    private fun extract(archivePath: Path, destination: Path) {
        val buffer = ByteArray(0x10000)
        ZipInputStream(Files.newInputStream(archivePath)).use { zis ->
            var zipEntry = zis.nextEntry
            while (zipEntry != null) {
                val newFile: Path = destination.resolve(zipEntry.name)
                Files.createDirectories(newFile.parent)
                Files.newOutputStream(newFile).use { outputStream ->
                    while (true) {
                        val read = zis.read(buffer)
                        if (read < 0) break
                        outputStream.write(buffer, 0, read)
                    }
                }
                zipEntry = zis.nextEntry
            }
            zis.closeEntry()
        }
    }

    init {
        temporaryDir = Files.createTempDirectory("flowLoader")
        extract(archivePath, temporaryDir)
        startQuasarAgent(archivePath, temporaryDir)
        val jars = (
                Files.list(temporaryDir.resolve("lib")).asSequence() +
                        Files.list(temporaryDir.resolve("drivers")).asSequence() +
                        Files.list(temporaryDir.resolve("cordapps")).asSequence()
                ).filter {
                    it.fileName.toString().endsWith(".jar")
                }.map {
                    FileSystems.newFileSystem(it, null)
                }.toList()
//        classLoader = getSystemClassLoader()
        classLoader = object : ClassLoader(parent) {

            override fun loadClass(name: String, resolve: Boolean): Class<*>? {
                synchronized(getClassLoadingLock(name)) {
                    var c = findLoadedClass(name)
                    if (c == null) {
                        if(name.startsWith("co.paralleluniverse")) {
                            //We have to return the same quasar classes used by the running java agent
                            c = Class.forName(name)
                        } else {
                            c = this.findClass(name)
                            if (c == null) {
                                c = super.loadClass(name, resolve)
                            } else if (resolve) {
                                resolveClass(c)
                            }
                        }
                    } else if (resolve) {
                        resolveClass(c)
                    }
                    return c
                }
            }


            private fun readAllBytes(inputStream : InputStream) : ByteArray {
                val result = ByteArrayOutputStream()
                val buffer = ByteArray(0x10000)
                while(true) {
                    val read = inputStream.read(buffer, 0, buffer.size)
                    if(read < 0) break
                    result.write(buffer, 0, read)
                }
                return result.toByteArray()
            }

            override fun findClass(name: String): Class<*>? {
                val resourceName = "/" + name.replace('.', '/') + ".class"
                return findResource(resourceName)?.let(URL::openStream)?.use { inputStream ->
                    val bytes = readAllBytes(inputStream)
                    defineClass(name, bytes, 0, bytes.size)
                }
            }

            override fun findResource(name: String): URL? {
                return jars.asSequence().map {
                    it.getPath(name)
                }.firstOrNull {
                    Files.exists(it)
                }?.toUri()?.toURL()
            }

            override fun getResource(name: String): URL? {
                return findResource(name) ?: super.getResource(name)
            }
        }
        Thread.currentThread().contextClassLoader = classLoader

        nodeSerializationEnv = SerializationEnvironment.with(
                SerializationFactoryImpl().apply
                {
                    registerScheme(AMQPServerSerializationScheme(
                            emptyList()))
                    registerScheme(AMQPClientSerializationScheme(
                            emptyList()))
                },
                p2pContext = AMQP_P2P_CONTEXT.withClassLoader(classLoader = classLoader),
                rpcServerContext = AMQP_RPC_SERVER_CONTEXT.withClassLoader(classLoader),
                rpcClientContext = AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader),
                storageContext = AMQP_STORAGE_CONTEXT.withClassLoader(classLoader),
                checkpointSerializer = KryoCheckpointSerializer,
                checkpointContext = KRYO_CHECKPOINT_CONTEXT.withClassLoader(classLoader)
        )


        val nodeServicesContext = newProxyObject<NodeServicesContext> { _: Any?, _: Method, _: Array<Any?>? ->
            TODO("Not yet implemented")
        }

        val serviceHub = newProxyObject<ServiceHub> { _: Any?, _: Method, _: Array<Any?>? ->
            TODO("Not yet implemented")
        }

        val res = classLoader.loadClass("co.paralleluniverse.fibers.instrument.JavaAgent").getMethod("isActive").invoke(null) as Boolean

        check(res) {
            "Missing the '-javaagent' JVM argument. Make sure you run the tests with the Quasar java agent attached to your JVM."
        }

        checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
                CheckpointSerializeAsTokenContextImpl(
                        nodeServicesContext,
                        CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
                        CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
                        serviceHub
                )
        )
    }

    fun fibers(): Sequence<String> = Files.list(temporaryDir.resolve("fibers")).asSequence()
            .map { it.fileName.toString() }
            .filter { it.endsWith(".fiber") }

    fun fiber(name: String): Any =
            Files.readAllBytes(temporaryDir.resolve("fibers/$name")).checkpointDeserialize(checkpointSerializationContext)

    private fun deletePath(path: Path?) {
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .map { obj: Path -> obj.toFile() }
                .forEach { obj: File -> obj.delete() }
    }

    override fun close() {
        deletePath(temporaryDir)
    }
}