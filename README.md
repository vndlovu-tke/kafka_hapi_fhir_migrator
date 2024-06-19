## Kafka to HAPI FHIR migration service

This service consumes data from the `migration` topic in kafka and posts the bundles to hapi-fhir on a given server

The service does the following

1. connects to migration topic
1. consumes messages in the topic in batches
1. posts message batches to hapi fhir
1. Saves processed IDs to a text file
1. Saves FHIR bundles that couldn't be posted to HAPI FHIR in a migration-errors Kafka topic and writes the document IDs to a file

## Steps to run

- Build the image:

```
./build_image.sh
```

- Set the placement node in the `docker-compose.yml` file to the appropriate node.

- Deploy into `migration` stack:

```shell
docker stack deploy -c docker/docker-compose.yml
```

- Deploy test postgres instance into hapi fhir stack:

```shell
docker stack deploy -c docker/docker-compose-postgres.yml hapi-fhir
```

- Update HAPI FHIR v5 service image and DB env vars:

```shell
`docker service update hapi-fhir_hapi-fhir --image jembi/hapi:v7.0.2-wget --env-add "spring.datasource.url=jdbc:postgresql://postgres-test-1:5432/hapi?targetServerType=primary"`
```

- Disable referential integrity checks to avoid dependency errors
