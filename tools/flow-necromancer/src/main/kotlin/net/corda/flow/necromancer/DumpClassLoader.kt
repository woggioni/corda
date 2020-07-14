package net.corda.flow.necromancer

import com.sun.tools.attach.VirtualMachine
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.internal.SerializationEnvironment
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.nodeapi.internal.lifecycle.NodeServicesContext
import net.corda.nodeapi.internal.rpc.client.AMQPClientSerializationScheme
import net.corda.nodeapi.internal.serialization.amqp.AMQPServerSerializationScheme
import net.corda.nodeapi.internal.serialization.kryo.KRYO_CHECKPOINT_CONTEXT
import net.corda.nodeapi.internal.serialization.kryo.KryoCheckpointSerializer
import net.corda.serialization.internal.AMQP_P2P_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_CLIENT_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_SERVER_CONTEXT
import net.corda.serialization.internal.AMQP_STORAGE_CONTEXT
import net.corda.serialization.internal.SerializationFactoryImpl
import sun.misc.CompoundEnumeration
import sun.misc.PerfCounter
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.File
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.net.URL
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.zip.ZipInputStream
import kotlin.Comparator
import kotlin.streams.asSequence

class DumpClassLoader(archivePath: Path, parent: ClassLoader = getSystemClassLoader()) : ClassLoader(parent), Closeable {

    companion object {
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
                val quasarJar = Files.list(temporaryDir.resolve("lib")).asSequence().firstOrNull {
                    it.fileName.toString().startsWith("quasar-core")
                } ?: throw RuntimeException("Quasar jar not found in $archivePath")
                val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().name
                val pid = nameOfRunningVM.substring(0, nameOfRunningVM.indexOf('@'))
                val vm = VirtualMachine.attach(pid)
                vm.loadAgent(quasarJar.toString(), quasarAgentArgs)
                vm.detach()
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

        private fun deletePath(path: Path?) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map { obj: Path -> obj.toFile() }
                    .forEach { obj: File -> obj.delete() }
        }

        private fun readAllBytes(inputStream: InputStream): ByteArray {
            val result = ByteArrayOutputStream()
            val buffer = ByteArray(0x10000)
            while (true) {
                val read = inputStream.read(buffer, 0, buffer.size)
                if (read < 0) break
                result.write(buffer, 0, read)
            }
            return result.toByteArray()
        }
    }

    val temporaryDir: Path
    val jars: Iterable<FileSystem>

    init {
        temporaryDir = Files.createTempDirectory("flowLoader")
        extract(archivePath, temporaryDir)
        startQuasarAgent(archivePath, temporaryDir)
        jars = (
                Files.list(temporaryDir.resolve("lib")).asSequence() +
                        Files.list(temporaryDir.resolve("drivers")).asSequence() +
                        Files.list(temporaryDir.resolve("cordapps")).asSequence()
                ).filter {
                    it.fileName.toString().endsWith(".jar")
                }.map {
                    FileSystems.newFileSystem(it, null)
                }.toList()
        startQuasarAgent(archivePath, temporaryDir)
    }

    fun summon(cb: DumpClassLoader.(necromancer: FlowNecromancer) -> Unit) {
        val cls = loadClass("net.corda.flow.necromancer.FlowNecromancerImpl", true)!!
        val constructor = cls.getConstructor(ClassLoader::class.java, Path::class.java)
        val necromancer = constructor.newInstance(this, temporaryDir)
        cb(necromancer as FlowNecromancer)
    }

//    override fun loadClass(name: String, resolve: Boolean): Class<*>? {
//        synchronized(getClassLoadingLock(name)) {
//            var c = findLoadedClass(name)
//            if (c == null) {
//                if(name.startsWith("co.paralleluniverse")) {
//                    //We have to return the same quasar classes used by the running java agent
//                    c = Class.forName(name)
//                } else {
//                    c = this.findClass(name)
//                    if (c == null) {
//                        c = super.loadClass(name, resolve)
//                    } else if (resolve) {
//                        resolveClass(c)
//                    }
//                }
//            } else if (resolve) {
//                resolveClass(c)
//            }
//            return c
//        }
//    }

    override fun loadClass(name: String, resolve: Boolean): Class<*>? {
        synchronized(getClassLoadingLock(name)) {
            var c = findLoadedClass(name)
            if (c == null) {
                c = when {
                    name.startsWith(FlowNecromancerImpl::class.java.name) -> {
                        val resourceName = name.replace('.', '/') + ".class"
                        getResource(resourceName)?.let(URL::openStream)?.use { inputStream ->
                            val bytes = readAllBytes(inputStream)
                            val cls = defineClass(name, bytes, 0, bytes.size)
                            if (resolve) {
                                resolveClass(cls)
                            }
                            cls
                        } ?: throw ClassNotFoundException(name)
                    }
                    else -> super.loadClass(name, resolve)
                }
            }
            return c
        }
    }

    override fun findClass(name: String): Class<*>? {
        val resourceName = name.replace('.', '/') + ".class"
        return findResource(resourceName)
                ?.let(URL::openStream)
                ?.use { inputStream ->
            val bytes = readAllBytes(inputStream)
            defineClass(name, bytes, 0, bytes.size)
        } ?: throw ClassNotFoundException(name)
    }

    override fun findResource(name: String): URL? = findResources(name).let {
        if (it.hasMoreElements()) it.nextElement() else null
    }

//    override fun getResource(name: String): URL? {
//        return findResource(name) ?: parent.getResource(name)
//    }

    override fun findResources(name: String): Enumeration<URL> = jars.asSequence().map {
        it.getPath(name)
    }.filter {
        Files.exists(it)
    }.map {
        it.toUri().toURL()
    }.iterator().let {
        return object : Enumeration<URL> {
            override fun hasMoreElements(): Boolean = it.hasNext()
            override fun nextElement(): URL = it.next()
        }
    }

//    override fun getResources(name: String): Enumeration<URL> {
//        return CompoundEnumeration(arrayOf(parent?.getResources(name), findResources(name)))
//    }

    override fun close() {
        deletePath(temporaryDir)
    }
}