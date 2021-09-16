# g2replicator

** UNDER CONSTRUCTION **

creates a datamart of the EDA reports in real time


[G2Replicator.py](G2Replicator.py) Core replicator code (not customizable)
[MyReplicator.py](MyReplicator.py) Customizable replicator functions (inherits thje core replicator)
[g2mart-schema-sqlite-create.sql.py](g2mart-schema-sqlite-create.sql.py) schema
[stream-replicator.py](stream-replicator.py) copy of stream-producer for replication

added to docker-environment-vars.sh

    # project related
    export SENZING_PROJECT_DIR=/home/jbutcher/senzing/projects/truth
    export SENZING_DATAMART_CONNECTION="sqlite3://na:na@/project/sqlite/G2Mart.db"
    export SENZING_DATAMART_REPLICATOR="/opt/senzing/g2/python/G2Replicator.py"

mapped project dir as volume in ...
    senzing-console.sh
    senzing-stream-producer.sh
    senzing-stream-loader.sh
--volume ${SENZING_PROJECT_DIR}:/project \
        

added parameters to ./senzing-stream-producer.sh and passed them in as environment variables

        # -- command line args
        if [ ! -z "$2" ]; then
            export SENZING_INPUT_URL=$2
        fi
        if [ ! -z "$3" ]; then
            export SENZING_DEFAULT_DATA_SOURCE=$3
        fi

        --env SENZING_DEFAULT_DATA_SOURCE=${SENZING_DEFAULT_DATA_SOURCE} \
        --env SENZING_INPUT_URL=${SENZING_INPUT_URL} \
  

    ./senzing-stream-producer.sh up /project/data/customer.json CUSTOMER

            fix=SENZING_SUBCOMMAND=csv-to-rabbitmq 

--------------------------------------

DEMO STEPS ...

step 0 - start rabbitmq

    ./senzing-rabbitmq.sh restart

step 1 - purge database in G2Command

    ./senzing-console.sh 

step 2 - bounce stream loader

    ./senzing-stream-loader.sh restart

step 3 - send in some data through stream producer 

    ./senzing-stream-producer.sh up /project/data/customer.json CUSTOMER




python3 stream-replicator.py rabbitmq --rabbitmq-queue senzing-rabbitmq-info-queue

to clean up everything
 docker system prune --volumes
