import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting occurrences
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

# Initialize data structures
reader_counts = defaultdict(set)  # Use a set to ensure each reader is counted once per book

# Set up live visuals
fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode for live updates

# Update chart function for live plotting
def update_chart():
    """Update the live chart with counts of unique readers for each book."""
    # Clear the previous chart
    ax.clear()

    # Prepare data for the chart
    books = list(reader_counts.keys())
    reader_counts_list = [len(readers) for readers in reader_counts.values()]

    # Create bar chart
    ax.bar(books, reader_counts_list, color="green")

    # Set labels and title
    ax.set_xlabel("Books")
    ax.set_ylabel("Number of Readers")
    ax.set_title('Number of Readers for Each Book')

    # Rotate x-axis labels and adjust layout
    ax.set_xticklabels(books, rotation=45, ha="right")
    plt.tight_layout()

    # Draw and pause briefly to update the chart
    plt.draw()
    plt.pause(0.01)

# Process a single message
def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.
    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            # Extract book information and reader
            author = message_dict.get("author", "Unknown Author")
            title = message_dict.get("title", "Unknown Title")
            reader = message_dict.get("reader", "Unknown Reader")

            # Use a tuple of (author, title) as the key, and add the reader to the set
            book_key = (author, title)
            reader_counts[book_key].add(reader)

            logger.info(f"Updated reader counts for '{author} - {title}': {len(reader_counts[book_key])} readers")
            
            # Update the chart
            update_chart()

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Main function to consume messages from Kafka
def main() -> None:
    """
    Main entry point for the consumer.
    - Reads Kafka topic and consumer group ID from environment variables.
    - Creates a Kafka consumer and processes messages.
    """
    logger.info("START consumer.")
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)
    logger.info(f"Polling messages from topic '{topic}'...")

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

# Conditional execution of main function
if __name__ == "__main__":
    main()

    # Turn off interactive mode after completion
    plt.ioff()

    # Display the final chart
    plt.show()
