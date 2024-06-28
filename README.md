# Real-Time Data Streaming
Full end-to-end data engineering pipeline that covers data ingestion, processing and storage.

## Data Sources
- randomuser.me

## Tech Stack
- Docker
- Apache Airflow
- PostgreSQL
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra

Start project:  
1. `source venv/bin/activate`  
2. `sudo docker compose up -d`

To populate requirements file:  
`pip freeze > requirements.txt`

To restart all containers and images:  
`docker-compose down --volumes --rmi all`
