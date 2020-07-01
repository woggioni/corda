package net.corda.tools.shell;

import org.crsh.cli.Command;
import org.crsh.cli.Man;
import org.crsh.cli.Named;
import org.crsh.cli.Usage;

import static net.corda.tools.shell.InteractiveShell.*;

@Named("checkpoints")
public class CheckpointShellCommand extends InteractiveShellCommand {

    @Command
    @Man("Outputs the contents of all checkpoints as json to be manually reviewed")
    @Usage("Outputs the contents of all checkpoints as json to be manually reviewed")
    public void dump() {
        runDumpCheckpoints(ops());
    }

    @Command
    @Man("Outputs the contents of all started flow checkpoints in a zip file")
    @Usage("Outputs the contents of all started flow checkpoints in a zip file")
    public void debug() {
        runDebugCheckpoints(ops());
    }
}
