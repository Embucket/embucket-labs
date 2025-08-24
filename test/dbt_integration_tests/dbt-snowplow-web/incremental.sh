echo "Setting up Docker container"
./setup_docker.sh

sleep 5

echo "Generating events"
python3 gen_events.py

echo "Loading events"
python3 load_events.py

echo "Running dbt"
./run_snowplow_web.sh

