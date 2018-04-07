package org.apache.hadoop.hbase.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@InterfaceAudience.Private
public abstract class ServerCommandLine extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ServerCommandLine.class);
    @SuppressWarnings("serial")
    private static final Set<String> DEFAULT_SKIP_WORDS = new HashSet<String>() {
        {
            add("secret");
            add("passwd");
            add("password");
            add("credential");
        }
    };

    protected abstract String getUsage();

    protected void usage(String message) {
        if (message != null) {
            System.err.println(message);
            System.err.println("");
        }

        System.err.println(getUsage());
    }

    public static void logJVMInfo() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" + runtime.getVmVendor()
                    + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
        }
    }

    public static void logProcessInfo(Configuration conf) {
        if (conf == null || !conf.getBoolean("hbase.envvars.logging.disabled", false)) {
            Set<String> skipWords = new HashSet<String>(DEFAULT_SKIP_WORDS);
            if (conf != null) {
                String[] confSkipWords = conf.getStrings("hbase.envvars.logging.skipwords");
                if (confSkipWords != null) {
                    skipWords.addAll(Arrays.asList(confSkipWords));
                }
            }
            nextEnv: for (Entry<String, String> entry : System.getenv().entrySet()) {
                String key = entry.getKey().toLowerCase(Locale.ROOT);
                String value = entry.getValue().toLowerCase(Locale.ROOT);
                // exclude variables which may contain skip words
                for (String skipWord : skipWords) {
                    if (key.contains(skipWord) || value.contains(skipWord))
                        continue nextEnv;
                }
                LOG.info("env:" + entry);
            }
        }
        logJVMInfo();
    }

    public void doMain(String args[]) {
        try {
            int ret = ToolRunner.run(HBaseConfiguration.create(), this, args);
            if (ret != 0) {
                System.exit(ret);
            }
        } catch (Exception e) {
            LOG.error("Failed to run", e);
            System.exit(-1);
        }
    }
}
