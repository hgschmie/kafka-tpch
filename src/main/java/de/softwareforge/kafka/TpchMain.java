/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.softwareforge.kafka;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.command.Cli;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.ParseException;

import javax.inject.Inject;

import java.util.concurrent.Callable;

import static com.google.common.base.Objects.firstNonNull;

public class TpchMain
{
    public static final Cli<TpchCommand> TPCH_PARSER;

    static {
        Cli.CliBuilder<TpchCommand> builder = Cli.<TpchCommand>builder("kafka-tpch")
                .withDescription("Kafka TPCH Loader")
                .withDefaultCommand(HelpCommand.class)
                .withCommand(LoadCommand.class)
                .withCommand(HelpCommand.class);

        TPCH_PARSER = builder.build();
    }

    public static void main(String[] args)
            throws Exception
    {
        try {
            System.exit(TPCH_PARSER.parse(args).call());
        }
        catch (ParseException e) {
            System.out.println(firstNonNull(e.getMessage(), "Unknown command line parser error"));
            System.exit(100);
        }
    }

    public abstract static class TpchCommand
            implements Callable<Integer>
    {
        @Override
        public final Integer call()
                throws Exception
        {
            try {
                execute();
                return 0;
            }
            catch (Exception e) {
                System.err.println(firstNonNull(e.getMessage(), "Unknown error"));
                return 1;
            }
        }

        @VisibleForTesting
        public abstract void execute()
                throws Exception;
    }

    @Command(name = "help", description = "Display help")
    public static class HelpCommand
            extends TpchCommand
    {
        @Inject
        public Help help;

        @Override
        public void execute()
                throws Exception
        {
            help.call();
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder("HelpCommand{help=").append(help).append('}');
            return sb.toString();
        }
    }
}
