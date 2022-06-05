
echo 'Getting db image'
docker pull postgres


echo 'Starting container'
docker run -d -p 5432:5432 --restart always -e POSTGRES_PASSWORD=mysecretpassword -e PGDATA=/var/lib/postgresql/data/pgdata -v ~/postgres_data:/var/lib/postgresql/data --name postgres_db postgres