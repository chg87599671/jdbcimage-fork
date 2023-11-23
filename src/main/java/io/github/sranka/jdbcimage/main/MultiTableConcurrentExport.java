package io.github.sranka.jdbcimage.main;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DB export that runs in multiple threads.
 */
public class MultiTableConcurrentExport extends SingleTableExport{
	Set<String> excludeTables = new HashSet<>();
	Set<String> includeTables = new HashSet<>();

	public void run(){
		// print platform concurrency, just FYI
		out.println("Concurrency: "+ concurrency);

		// setup tables to export
		setTables(getUserTables().stream().collect(Collectors.toMap(Function.identity(), Function.identity())), out);

		Optional.ofNullable(System.getProperty("exclude")).ifPresent(x -> excludeTables.addAll(Arrays.stream(x.toUpperCase().split(",")).collect(Collectors.toSet())));
		Optional.ofNullable(System.getProperty("include")).ifPresent(x -> includeTables.addAll(Arrays.stream(x.toUpperCase().split(",")).collect(Collectors.toSet())));
		// runs export concurrently
		out.println("Exporting table files to: "+ getBuildDirectory());
		List<Callable<?>> callableList = tables.entrySet().stream()
				.filter(x -> excludeTables.isEmpty() || !excludeTables.contains(x.getKey()))
				.filter(x -> includeTables.isEmpty() || includeTables.contains(x.getKey()))
				.map(x -> getExportTask(x.getKey(), x.getValue()))
				.collect(Collectors.toList());
		run(callableList);
		zip();
	}

	private Callable<?> getExportTask(String tableName, String fileName){
		return () -> {
			boolean failed = true;
			try{
				long start = System.currentTimeMillis();
				long rows = exportTable(tableName, new File(getBuildDirectory(), fileName));
				out.println("SUCCESS: Exported table "+tableName + " - "+rows+" rows in " + Duration.ofMillis(System.currentTimeMillis()-start));
				failed = false;
			} finally {
				if (failed){
					// exception state, notify other threads to stop reading from queue
					out.println("FAILURE: Export of table "+tableName);
				}
			}
			return null;
		};
	}

	public static void main(String... args) throws Exception{
		args = setupSystemProperties(args);

		try(MultiTableConcurrentExport tool = new MultiTableConcurrentExport()){
			tool.setupZipFile(args);
			tool.run();
		}
	}
}
