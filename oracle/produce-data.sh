docker run --tty \
           --network oracle_kafka-connect \
           -v /Users/aqua-len/IdeaProjects/jdbs-kafka-connectors/oracle/data/ASN_20210412164801960.xml:/ASN_20210412164801960.xml\
           confluentinc/cp-kafkacat \
           bash -c "
            cat /ASN_20210412164801960.xml | kafkacat  \
           -b broker:29092 \
            -P -t main"
