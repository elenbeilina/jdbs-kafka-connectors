docker run --tty \
           --network sink_kafka-connect \
           -v /Users/aqua-len/Downloads/jdbs-kafka-postgres-connector/sink/data/test-data-marvel.json:/test-data-marvel.json\
           confluentinc/cp-kafkacat \
           bash -c "
            cat /test-data-marvel.txt | kafkacat  \
           -b broker:29092 \
            -P -t main \
            -H publisher='Marvel Comics'"
