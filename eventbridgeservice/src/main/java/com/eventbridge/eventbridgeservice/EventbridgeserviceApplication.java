package com.eventbridge.eventbridgeservice;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.AmazonEventBridgeClient;
import com.amazonaws.services.eventbridge.model.PutEventsRequest;
import com.amazonaws.services.eventbridge.model.PutEventsRequestEntry;
import com.amazonaws.services.eventbridge.model.PutEventsResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;

@SpringBootApplication
public class EventbridgeserviceApplication {

	public static void main(String[] args) {

		String access_key_id="AKIAXYKJR2VIHN4GPJGU";
		String secret_key_id="koEI27+nu1zGXLMcY2aQSgjTg9vMe6pC0bKuY8xS";

		SpringApplication.run(EventbridgeserviceApplication.class, args);

		final String queueUrl = "https://sqs.us-east-1.amazonaws.com/533267076432/SNSToEventBridgeQueue";

		// Create an SQS client
		AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().
				withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access_key_id, secret_key_id)))
				.build();

		// Receive messages from the SQS queue
		while (true){
		ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
				.withQueueUrl(queueUrl)
				.withMaxNumberOfMessages(10); // Adjust as needed

		ReceiveMessageResult receiveResult = sqsClient.receiveMessage(receiveRequest);
		for (Message message : receiveResult.getMessages()) {
			// Process the message
			System.out.println("Received message: " + message.getBody());

			//send to eventBridge
			AmazonEventBridge amazonEventBridge= AmazonEventBridgeClient.builder()
					.withRegion(Regions.US_EAST_1)
					.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access_key_id, secret_key_id)))
					.build();

			PutEventsRequestEntry putEventsRequestEntry=new PutEventsRequestEntry();
			putEventsRequestEntry.withSource("email-event")
					.withDetailType("email-notification")
					.withDetail(message.getBody())
					.withEventBusName("email-event-bus");
			PutEventsRequest req=new PutEventsRequest();
			req.withEntries(putEventsRequestEntry);

			PutEventsResult result=amazonEventBridge.putEvents(req);
			System.out.println(result);

			//send to eventBridge



			// Delete the message from the queue once processed
			//sqsClient.deleteMessage(queueUrl, message.getReceiptHandle());
		}

		}
	}



}
