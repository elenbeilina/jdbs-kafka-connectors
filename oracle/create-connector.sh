printf " -> Try to a connect delete if exists \n\n"
curl \
  --request DELETE \
  --header "Content-Type: application/json" \
  http://localhost:8083/connectors/jdbc-sink-kafka-connector

printf "\n\n -> Create sink connector \n\n"
curl \
  --request POST \
  --header "Content-Type: application/json" \
  --data @sink.json \
  http://localhost:8083/connectors