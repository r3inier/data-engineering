# data-engineering

Start project:
source venv/bin/activate
sudo docker compose up -d

To populate req file: 
pip freeze > requirements.txt

To restart all containers and images:
docker-compose down --volumes --rmi all