### Question 1: Understanding Docker images

```bash
docker run -it --rm --entrypoint bash python:3.13
pip --version



### Question — Docker Compose Networking (pgAdmin → Postgres)

**Task:**  
Given the provided `docker-compose.yaml`, determine the correct hostname and port that pgAdmin should use to connect to the Postgres database.

**Answer:**

```bash
Hostname: db
Port: 5432
