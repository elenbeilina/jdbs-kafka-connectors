### App for moving data form kafka topic to oracle database table.

---
#### Environment:
- docker env can be found in `docker-compose.yaml` file
- control-center was added to env for cluster monitoring, \
  so cluster state can be checked by `http://localhost:9021/clusters` url.

---
#### Test scenario:

 1. Go to postgres directory:
    ```
    cd postgres
    ```
 2. Run docker environment:
    ```
    docker compose up -d
    ```
 3. Create connector:
    ```
    sh create-connector.sh
    ```
 4. Produce test data to kafka topic:
    ```
    sh produce-data.sh
    ```
    
Result can be checked via connecting to database.

**Database properties**:

URL: `jdbc:postgresql://localhost:5432/postgres` \
username: `adidas` \
password: `pas`