package maven.servicebus.read;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class readFromTopicv4 {

	/*
	 * See https://github.com/Azure/azure-service-bus/blob/master/samples/Java/azure-servicebus/TopicsGettingStarted/src/main/java/com/microsoft/azure/servicebus/samples/topicsgettingstarted/TopicsGettingStarted.java
	 *
	 * In case maven is used, add following to pom.xml, otherwise add as external JARs
	 *
	 * <dependency>
     *    <groupId>com.microsoft.azure</groupId>
     *    <artifactId>azure-servicebus</artifactId>
     *	  <version>1.0.0</version>
	 * </dependency>
	 *
	 */
	
    private static final String connectionString = "Endpoint=sb://<<your endpoint>>.servicebus.windows.net/;SharedAccessKeyName=<<your key name here>>;SharedAccessKey=<<your key here>>;EntityPath=<<your path here>>";
    public static void main(String[] args) throws Exception {

    	SubscriptionClient subscription1Client; 	
        subscription1Client = new SubscriptionClient(new ConnectionStringBuilder(connectionString, "genericroadtopic/subscriptions/schipholasb"), ReceiveMode.PEEKLOCK);
        registerMessageHandlerOnClient(subscription1Client);   
    }
    
    static void registerMessageHandlerOnClient(final SubscriptionClient receiveClient) throws Exception {
        // register the RegisterMessageHandler callback
        receiveClient.registerMessageHandler(
                new IMessageHandler() {
                    // callback invoked when the message handler loop has obtained a message
                    public CompletableFuture<Void> onMessageAsync(IMessage message)  {
                        // receives message is passed to callback

                        byte[] body = message.getBody();
                        try {                        	
                        	String rawText = new String(body, "UTF-8");
                        	System.out.printf(rawText + "\r\n");
                        }
                        catch (Exception e){
                        	System.out.printf(e.getMessage());
                        }

                        return receiveClient.completeAsync(message.getLockToken());
                    }

                    // callback invoked when the message handler has an exception to report
                    public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                        System.out.printf(exceptionPhase + "-" + throwable.getMessage());
                    }
                },

                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));
    }   
}