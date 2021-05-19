docker run --tty \
           --network postgres_kafka-connect \
           -v /Users/aqua-len/IdeaProjects/jdbs-kafka-connectors/postgres/data/test-data-marvel.txt:/test-data-marvel.txt\
           confluentinc/cp-kafkacat \
           bash -c "
            cat /test-data-marvel.txt | kafkacat  \
           -b broker:29092 \
            -P -t main \
            -H publisher='Marvel Comics'"
