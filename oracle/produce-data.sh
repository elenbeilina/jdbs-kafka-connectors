docker run --tty \
           --network oracle_kafka-connect \
           -v /Users/aqua-len/IdeaProjects/jdbs-kafka-connectors/oracle/data/test-data-marvel.txt:/test-data-marvel.txt\
           confluentinc/cp-kafkacat \
           bash -c "
            cat /test-data-marvel.txt | kafkacat  \
           -b broker:29092 \
            -P -t main \
            -H publisher='Marvel Comics'"
