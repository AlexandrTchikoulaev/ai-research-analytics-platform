# Projeto-Final

# Website
iniciar: uvicorn website.api:app --reload 

# GitHub
Sempre que forem feitas alterações:
git add .
git commit -m "mensagem"
git push

# Ligações

Postgres
conn = psycopg2.connect(
    host="localhost",
    port="5433",  # ajusta se for diferente
    dbname="projeto_db",
    user="projeto_utilizador",
    password="projeto"
)

Minio
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='admin123'
)

# Processo
(python extra/delete_setup.py)
python códigos_gerais/setup.py
python populate/populate_opdb_csv.py
python etl/etl_wov.py