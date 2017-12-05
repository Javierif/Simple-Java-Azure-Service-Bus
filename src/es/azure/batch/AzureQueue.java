package es.azure.batch;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.google.gson.Gson;

import java.time.Duration;
import java.util.concurrent.*;

public class AzureQueue {

	static final Gson GSON = new Gson();

	private static final String connectionString = "Endpoint=sb://testJava.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<CambiarPorTuAccessKey>";
	private static final String queueName = "testJavaqueue";

	public void run() throws Exception {

		IMessageReceiver receiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
				new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.RECEIVEANDDELETE);
		this.receiveMessagesAsync(receiver);

	}

	CompletableFuture<?> receiveMessagesAsync(IMessageReceiver receiver) {

		CompletableFuture currentTask = new CompletableFuture();

		try {
			CompletableFuture.runAsync(() -> {
				while (!currentTask.isCancelled()) {
					try {
						IMessage message = receiver.receive(Duration.ofSeconds(60));
						if (message != null) {
							byte[] body = message.getBody();
							System.out.println(new String(body).substring(63));
						}
					} catch (Exception e) {
						currentTask.completeExceptionally(e);
					}
				}
				currentTask.complete(null);
			});
			return currentTask;
		} catch (Exception e) {
			currentTask.completeExceptionally(e);
		}
		return currentTask;
	}

	public static void main(String[] args) {

		AzureQueue app = new AzureQueue();
		try {
			app.run();
		} catch (Exception e) {
			System.out.printf("%s", e.toString());
		}
	}

}
