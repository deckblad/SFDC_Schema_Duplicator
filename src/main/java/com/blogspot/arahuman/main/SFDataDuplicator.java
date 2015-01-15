package com.blogspot.arahuman.main;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.blogspot.arahuman.helper.CmdLineArgs;
import com.blogspot.arahuman.sf.SfDownloader;

public class SFDataDuplicator {

	private static final Logger logger = Logger.getLogger(SFDataDuplicator.class);

	//private static Options options = new Options();

	/**
	 * Apply Apache Commons CLI GnuParser to command-line arguments.
	 * 
	 * @param commandLineArguments
	 *            Command-line arguments to be processed with Gnu-style parser.
	 */
	public static CmdLineArgs useGnuParser(final String[] commandLineArguments) {
		logger.debug(commandLineArguments.toString());
		final CommandLineParser cmdLineGnuParser = new GnuParser();
		CmdLineArgs cmd = new CmdLineArgs();
		final Options gnuOptions = constructGnuOptions();
		CommandLine commandLine;
		try {
			commandLine = cmdLineGnuParser.parse(gnuOptions, commandLineArguments);
			if (commandLine.hasOption("p"))
				cmd.setEnableProxy(Boolean.parseBoolean(commandLine.getOptionValue("p")));
			if (commandLine.hasOption("pdb"))
				cmd.setPopulateDb(Boolean.parseBoolean(commandLine.getOptionValue("pdb")));
			if (commandLine.hasOption("ph")) 
				cmd.setProxyHost(commandLine.getOptionValue("ph"));
			if (commandLine.hasOption("po")) 
				cmd.setProxyPort(commandLine.getOptionValue("po"));
			if (commandLine.hasOption("pu")) 
				cmd.setProxyUser(commandLine.getOptionValue("pu"));
			if (commandLine.hasOption("pp")) 
				cmd.setProxyPassword(commandLine.getOptionValue("pp"));
			
			if (commandLine.hasOption("sfu")) 
				cmd.setSfUser(commandLine.getOptionValue("sfu"));
			if (commandLine.hasOption("sfp")) 
				cmd.setSfPassword(commandLine.getOptionValue("sfp"));
			if (commandLine.hasOption("sft")) 
				cmd.setSfToken(commandLine.getOptionValue("sft"));
			if (commandLine.hasOption("sfe")) 
				cmd.setSfEndpoint(commandLine.getOptionValue("sfe"));
				//cmd.setSfEndpoint("https://test.salesforce.com/services/Soap/u/23.0");
			
			if (commandLine.hasOption("dbu")) 
				cmd.setDbUser(commandLine.getOptionValue("dbu"));
			if (commandLine.hasOption("dbp")) 
				cmd.setDbPassword(commandLine.getOptionValue("dbp"));
			if (commandLine.hasOption("dbh")) 
				cmd.setDbHost(commandLine.getOptionValue("dbh"));
			if (commandLine.hasOption("dbs")) 
				cmd.setDbSchema(commandLine.getOptionValue("dbs"));

			if (commandLine.hasOption("sp")) 
				cmd.setSqlPath(commandLine.getOptionValue("sp"));
			if (commandLine.hasOption("stbl")) 
				cmd.setSqlTablefile(commandLine.getOptionValue("stbl"));
			if (commandLine.hasOption("srel")) 
				cmd.setSqlRelationfile(commandLine.getOptionValue("srel"));
			if (commandLine.hasOption("spr")) 
				cmd.setSqlPrefix(commandLine.getOptionValue("spr"));
			
			if (commandLine.hasOption("csql"))
				cmd.setCreateLocalSqlfiles(Boolean.parseBoolean(commandLine.getOptionValue("csql")));
			if (commandLine.hasOption("cdbs"))
				cmd.setCreateDbSchema(Boolean.parseBoolean(commandLine.getOptionValue("cdbs")));
			if (commandLine.hasOption("pdf"))
				cmd.setPopulateDb(Boolean.parseBoolean(commandLine.getOptionValue("pdf")));
			if (commandLine.hasOption("dte"))
				cmd.setTableExistsThenDrop(Boolean.parseBoolean(commandLine.getOptionValue("dte")));
			
			return cmd;
		} catch (ParseException parseException) // checked exception
		{
			logger.error("Encountered exception while parsing using GnuParser:\n" + parseException.getMessage());
			return null;
		}
	}

	/**
	 * Construct and provide GNU-compatible Options.
	 * 
	 * @return Options expected from command-line of GNU form.
	 */
	public static Options constructGnuOptions() {
		final Options gnuOptions = new Options();

		gnuOptions.addOption("p", "enable-proxy", true, "Enable proxy[true|false]")

		.addOption("ph", "proxy-host", true, "Enter the proxy host name")
		.addOption("po", "proxy-port", true, "Enter the proxy host name")
		.addOption("pa", "proxy-auth-req", true, "Is Proxy authentication required")
		.addOption("pu", "proxy-user", true, "Enter the proxy user name")
		.addOption("pp", "proxy-password", true, "Enter the proxy password")

		.addOption("sfu", "sf-user", true, "Enter the salesforce user name")
		.addOption("sfp", "sf-password", true, "Enter the salesforce password")
		.addOption("sft", "sf-token", true, "Enter the salesforce token")
		.addOption("sfe", "sf-endpoint", true, "Enter the salesforce endpoint(default: sandbox)")

		.addOption("dbu", "db-user", true, "Enter the mysql user name")
		.addOption("dbp", "db-password", true, "Enter the mysql password")
		.addOption("dbh", "db-host", true, "Enter the mysql host name")
		.addOption("dbs", "db-schema", true, "Enter the mysql schema name")

		.addOption("sp", "sql-path", true, "Enter the path to store mysql queries")
		.addOption("stbl", "sql-table-file", true, "Enter the file name for storing table queries")
		.addOption("srel", "sql-relation-file", true, "Enter the file name for storing relation queries")
		.addOption("spr", "sql-prefix", true, "Enter the prefix for creating tables in mysql")

		.addOption("csql", "create-local-sqlfiles", true, "Enable creating local sql files from salesforce")
		.addOption("cdbs", "create-db-schema", true, "Enable creating local schema from local sql files")
		.addOption("pdb", "populate-db", true, "Enable populating data from salesforce")
		.addOption("dte", "drop-table-if-exists", true, "Drop table if already exists");

		return gnuOptions;
	}

	/**
	 * Display command-line arguments without processing them in any further
	 * way.
	 * 
	 * @param commandLineArguments
	 *            Command-line arguments to be displayed.
	 */
	public static void displayProvidedCommandLineArguments(final String[] commandLineArguments, final OutputStream out) {
		final StringBuffer buffer = new StringBuffer();
		for (final String argument : commandLineArguments) {
			buffer.append(argument).append(" ");
		}
		try {
			out.write((buffer.toString() + "\n").getBytes());
		} catch (IOException ioEx) {
			System.err.println("WARNING: Exception encountered trying to write to OutputStream:\n" + ioEx.getMessage());
			System.out.println(buffer.toString());
		}
	}

	/**
	 * Display example application header.
	 * 
	 * @out OutputStream to which header should be written.
	 */
	public static void displayHeader(final OutputStream out) {
		final String header = "[Salesforce - Local DB data replicator from arahuman.blogspot.com]\n";
		try {
			out.write(header.getBytes());
		} catch (IOException ioEx) {
			System.out.println(header);
		}
	}

	/**
	 * Write the provided number of blank lines to the provided OutputStream.
	 * 
	 * @param numberBlankLines
	 *            Number of blank lines to write.
	 * @param out
	 *            OutputStream to which to write the blank lines.
	 */
	public static void displayBlankLines(final int numberBlankLines, final OutputStream out) {
		try {
			for (int i = 0; i < numberBlankLines; ++i) {
				out.write("\n".getBytes());
			}
		} catch (IOException ioEx) {
			for (int i = 0; i < numberBlankLines; ++i) {
				System.out.println();
			}
		}
	}

	/**
	 * Print usage information to provided OutputStream.
	 * 
	 * @param applicationName
	 *            Name of application to list in usage.
	 * @param options
	 *            Command-line options to be part of usage.
	 * @param out
	 *            OutputStream to which to write the usage information.
	 */
	public static void printUsage(final String applicationName, final Options options, final OutputStream out) {
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter usageFormatter = new HelpFormatter();
		usageFormatter.printUsage(writer, 80, applicationName, options);
		writer.flush();
	}

	/**
	 * Write "help" to the provided OutputStream.
	 */
	public static void printHelp(final Options options, final int printedRowWidth, final String header, final String footer, final int spacesBeforeOption, final int spacesBeforeOptionDescription,
			final boolean displayUsage, final OutputStream out) {
		final String commandLineSyntax = "java -cp sfdata_duplicator.jar";
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax, header, options, spacesBeforeOption, spacesBeforeOptionDescription, footer, displayUsage);
		writer.flush();
	}

	/**
	 * Main executable method used to demonstrate Apache Commons CLI.
	 * 
	 * @param commandLineArguments
	 *            Commmand-line arguments.
	 */
	//public static void main(final String[] commandLineArguments) {
	public static void main(String[] commandLineArguments) {
		logger.info("Starting the applicaton to downloaded data from Salesforce.com" );
		final String applicationName = "SFDataDuplicator";
		displayBlankLines(1, System.out);
		displayHeader(System.out);
		displayBlankLines(2, System.out);
		if (commandLineArguments.length < 8) {
			System.out.println("-- USAGE --");
			// printUsage(applicationName + " (Posix)", constructPosixOptions(),
			// System.out);
			displayBlankLines(1, System.out);
			printUsage(applicationName + " (Gnu)", constructGnuOptions(), System.out);
			displayBlankLines(4, System.out);
			System.out.println("-- HELP --");
			// printHelp(
			// constructPosixOptions(), 80, "POSIX HELP", "End of POSIX Help",
			// 3, 5, true, System.out);
			displayBlankLines(1, System.out);
			printHelp(constructGnuOptions(), 80, " GNU HELP", "End of GNU Help", 5, 3, true, System.out);
		}
		displayProvidedCommandLineArguments(commandLineArguments, System.out);
		
		CmdLineArgs cmd = useGnuParser(commandLineArguments);
		cmd.printArguments();
		if (!cmd.validate()) {
			logger.error(cmd.getErrorMessage());
		}
		SfDownloader sfd = new SfDownloader(cmd);
		sfd.execute();
		logger.info("Successfully downloaded data from Salesforce.com" );
	}

}

// /**
// * Construct and provide Posix-compatible Options.
// *
// * @return Options expected from command-line of Posix form.
// */
// public static Options constructPosixOptions()
// {
// final Options posixOptions = new Options();
// posixOptions.addOption("display", false, "Display the state.");
// return posixOptions;
// }

/**
 * Apply Apache Commons CLI PosixParser to command-line arguments.
 * 
 * @param commandLineArguments
 *            Command-line arguments to be processed with Posix-style parser.
 */
// public static void usePosixParser(final String[] commandLineArguments)
// {
// final CommandLineParser cmdLinePosixParser = new PosixParser();
// final Options posixOptions = constructPosixOptions();
// CommandLine commandLine;
// try
// {
// commandLine = cmdLinePosixParser.parse(posixOptions, commandLineArguments);
// if ( commandLine.hasOption("display") )
// {
// System.out.println("You want a display!");
// }
// }
// catch (ParseException parseException) // checked exception
// {
// System.err.println(
// "Encountered exception while parsing using PosixParser:\n"
// + parseException.getMessage() );
// }
// }
