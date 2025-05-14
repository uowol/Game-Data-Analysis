Build the image:
```sh
docker build . --tag metaduck:latest
```

Then create the container:
```sh
docker run --name metaduck -d -p 3000:3000 -m 2GB -e MB_PLUGINS_DIR=/home/plugins -v {base_dir}/data:/home/data metaduck
```