### App for moving xml data form kafka topic to oracle database table.

---
#### Environment:
- docker env can be found in `docker-compose.yaml` file
- control-center was added to env for cluster monitoring, \
  so cluster state can be checked by `http://localhost:9021/clusters` url.
- project is using SMTs:
  - xml-transformation from confluent: https://github.com/jcustenborder/kafka-connect-transform-xml
  - xml-flattener for our specific data case that is located in `xml-flattener` module
  
- instructions for getting oracle docker image: https://www.petefreitag.com/item/886.cfm

---
#### Test scenario:
1. Go to xml-flattener directory:
    ```
    cd xml-flattener
    ```
2. Build xml-flattener SMT:
    ```
    mvn clean package
    ```
3. Return to the main directory:
    ```
    cd 
    ```
 4. Go to oracle directory:
    ```
    cd oracle
    ```
 5. Run docker environment:
    ```
    docker compose up -d
    ```
 6.Create connector:
    ```
    sh create-connector.sh
    ```
 7. Change local path to `ASN.xsd` in `produce-data.sh`


 5. Produce test data to kafka topic:
    ```
    sh produce-data.sh
    ```
    
Result can be checked via connecting to database.

**Database properties**:

URL: `jdbc:oracle:thin:@localhost:11521:XE` \
username: `SYSTEM` \
password: `testing12345`