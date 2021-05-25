docker run --tty \
           --network oracle_kafka-connect \
           -v /Users/aqua-len/IdeaProjects/jdbs-kafka-connectors/oracle/data/Info.xml:/Info.xml\
           confluentinc/cp-kafkacat \
           bash -c "
            cat /Info.xml | kafkacat  \
           -b broker:29092 \
            -P -t main"
