### ZTF alerts demo

#### Download and install Docker and Robo 3T

- Download and install the appropriate version of Docker from [here](https://www.docker.com/community-edition). 
  You will need to create an account on their website.
- Download and install Robo 3T from [here](https://robomongo.org/download).
  We will use it to connect to the database.

#### Fetch, build, and run the code

Clone the repo and cd to the directory:
```bash
git clone https://github.com/dmitryduev/ztf-alerts-demo.git
cd ztf-alerts-demo
```

Create a persistent Docker volume for MongoDB:
```bash
docker volume create alert-fetcher-mongo-volume
```

Launch the MongoDB container. Feel free to change u/p for the admin
```bash
docker run -d --restart always --name alert-fetcher-mongo -p 27018:27017 -v alert-fetcher-mongo-volume:/data/db \
       -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
       mongo:latest
```

Build and launch the alert-fetcher container. Bind-mount a directory on the host machine to store the alerts:
```bash
cd alert-fetcher
docker build -t alert-fetcher -f Dockerfile .
docker run -v /path/to/store/alerts:/alerts \
           --name alert-fetcher -d --link alert-fetcher-mongo:mongo --restart always alert-fetcher
```

