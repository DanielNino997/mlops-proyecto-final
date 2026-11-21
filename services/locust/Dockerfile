FROM locustio/locust:latest
COPY locustfile.py payloads.json /mnt/locust/
# El host de la API se pasa con LOCUST_HOST (ej. http://api:8000); UI en el puerto 8089
CMD ["-f", "/mnt/locust/locustfile.py"]
