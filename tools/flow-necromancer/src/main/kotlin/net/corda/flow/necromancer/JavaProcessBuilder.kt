package net.corda.flow.necromancer

import java.nio.file.Paths
import java.util.*

class JavaProcessBuilder(private val mainClass: String) {
    var javaHome = System.getProperty("java.home")
    var classPath = System.getProperty("java.class.path")
    var properties = Properties()
    var cliArgs: Array<out String>? = null
    var jvmArgs : List<String> = emptyList()

    fun exec(customize: ProcessBuilder.() -> Unit = {}): Process {
        val javaBin = Paths.get(javaHome, "bin", "java")
        val propertySequence = properties.let {
           it.entries.asSequence()
        }.map { entry ->
            String.format("-D%s=%s", entry.key, entry.value)
        }
        val cmd: List<String> = (sequenceOf(javaBin.toString(), "-cp", classPath) +
                jvmArgs.asSequence() +
                propertySequence +
                sequenceOf(mainClass) +
                (cliArgs?.asSequence() ?: emptySequence<String>())).toList()
        return ProcessBuilder(cmd).also(customize).start()
    }
}