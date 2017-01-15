
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * A class representing the time limited execution for a Azure api client
 * request.
 * 
 * @author pamela sanchez
 */
public class TimeCheckedRequestExecutor {
	private static final Logger logger = Logger.getLogger(TimeCheckedRequestExecutor.class.getName());
	private final ExecutorService executor;
	private long requestTimeMillis;

	/**
	 * Constructor for this class.
	 * 
	 * @param threadPoolSize
	 *            number of threads to be created
	 */
	public TimeCheckedRequestExecutor() {
		this.executor = Executors.newFixedThreadPool(1);
	}

	/**
	 * Runs Callable task to perform specified task through Azure Java SDK.
	 * 
	 * @param callable
	 *            task to be run and stopped at timeout
	 * @param timeout
	 *            amount of time to run the task
	 * @param timeUnit
	 *            unit for timeout amount
	 * @return object of type returned by task
	 * @throws Exception
	 *             exception at timeout
	 */
	private <T> T run(final Callable<T> callable, final long timeout, final TimeUnit timeUnit) throws Exception {

		final Future<T> future = this.executor.submit(callable);
		try {
			return future.get(timeout, timeUnit);
		} catch (final Exception e) {
			/*
			 * this call will close any fd that had been opened from inside the
			 * callable task
			 */
			future.cancel(true);
			throw e;
		}
	}

	/**
	 * Starts the time limited azure request
	 * 
	 * @param callableQuery
	 *            Callable task object containing query to be run
	 * @param timeoutMillis
	 *            time limit for Callable task in milliseconds
	 * @param timegrain
	 *            validity time of metric
	 * @param datetime
	 *            date time of request
	 * @return
	 * @return map with metric values
	 */
	public <T> T startTimeLimitedRequest(Callable<T> callableQuery, long timeout, String logPrefix, TimeUnit timeunit) {

		if (logPrefix == null) {
			logPrefix = "Time limited Request: ";
		}

		if (callableQuery == null || timeout == 0 || timeunit == null) {
			logger.warning("Cannot start with arguments provided.");
			return null;
		}

		try {
			long start = System.currentTimeMillis();
			T valueResult = this.run(callableQuery, timeout, timeunit);
			long end = System.currentTimeMillis();
			this.requestTimeMillis = end - start;
			return valueResult;
		} catch (TimeoutException e) {
			logger.warning(logPrefix + "Timed out after " + timeout + " " + timeunit + ".");
		} catch (Exception e) {
			logger.warning(logPrefix + "Non-timeout exception occured");
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
		return null;
	}

	/**
	 * Clean up
	 * 
	 */
	private void shutdown() {
		try {
			this.executor.shutdownNow();
		} catch (Exception e) {
			logger.warning("Exception occured, shut down executor unsuccessfully.");
		}
	}

	/**
	 * Gets the time it took to run the request task
	 * 
	 * @return time in milliseconds
	 */
	public long getRequestTime() {
		return requestTimeMillis;
	}

}