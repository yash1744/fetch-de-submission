import os
from user_login_processor import UserLoginProcessor
from src.logger import logger

if __name__ == "__main__":
    # Configuration from environment variables
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC", "user-login")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "processed-user-login")
    DLQ_TOPIC = os.getenv("DLQ_TOPIC", "user-login-dlq")
    
    logger.info("Starting user login processor...")
    processor = UserLoginProcessor(
        BOOTSTRAP_SERVERS,
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        DLQ_TOPIC
    )
    
    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down processor...")
        processor.cleanup()
