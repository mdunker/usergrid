package org.apache.usergrid.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassIndexGenerator extends ToolBase {

    private static final Logger logger = LoggerFactory.getLogger( CassIndexGenerator.class );

    public static final String NUM_WORKERS_ARG = "numWorkers";

    @Override
    @SuppressWarnings( "static-access" )
    public Options createOptions() {

        Options options = super.createOptions();

        Option numWorkersOption = OptionBuilder.withArgName(NUM_WORKERS_ARG).hasArg().isRequired(true)
            .withDescription("number of workers").create(NUM_WORKERS_ARG);
        options.addOption(numWorkersOption);

        return options;

    }

    @Override
    public void runTool(CommandLine line) throws Exception {

        startSpring();

    }

}
