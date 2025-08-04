package com.mycompany;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.json.JSONObject;

public class HelloStreamTask implements StreamTask {

  // Define the output stream where we will send processed messages.
  // This matches the "processed-events" topic we create in Kafka.
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "processed-events");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // Get the incoming message, which is expected to be a string in JSON format.
    String incomingMessage = (String) envelope.getMessage();

    try {
      // Parse the JSON string into a JSON object.
      JSONObject event = new JSONObject(incomingMessage);

      // Extract the 'user' and 'action' fields from the JSON object.
      String user = event.optString("user", "unknown").toUpperCase();
      String action = event.optString("action", "unknown").toUpperCase();

      // Create the processed output message.
      String outputMessage = String.format("USER '%s' HAS %sED.", user, action);

      // Send the processed message to the output stream.
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outputMessage));

    } catch (Exception e) {
      // If there's an error (e.g., malformed JSON), print an error message.
      // In a real application, you would handle this more robustly.
      System.err.println("Failed to process message: " + incomingMessage);
      e.printStackTrace();
    }
  }
}
