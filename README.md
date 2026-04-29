# Simple Kafka Project

### Setup
1. Clone the repository
2. Create a virtual environment
    ```
    python -m venv .venv
    ```

    ```
    .\.venv\Scripts\activate
    ```

3. Install the dependencies
    ```
    pip install -r requirements.txt
    ```

4. Run the docker-compose file
    ```
    docker-compose up -d
    ```

5. Run the producer and consumer scripts
    ```
    python producer.py
    python consumer.py
    ```


## Note:
a. Make sure to have docker installed and running before running the docker-compose file.
b. Create data.json and customize producer.py and consumer.py to fit your needs.