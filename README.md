# NATS Gateway

Demo client that converts synchronous request/response paradigm into asynchronous publish/subscribe paradigm.


# Development

Requires:

  - Python >= 3.7
  - Pipenv (`pip install pipenv`)


### Running NATS Server

Start NATS server:

```
docker-compose up -d
```

Browse to http://localhost:8222 for NATS UI.


Stop NATS server and clean up:


```
docker-compose down
```

### Running Demo Script

(Requires running NATS Server)

```
pipenv install --dev
pipenv shell
python nats_gateway.py
```
