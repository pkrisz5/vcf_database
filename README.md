# vcf_database

The purpose of this project is to document how to set up the postgres database hosted at ebi.

## Prerequisities

Certain services and resources need to be setup and be operational to take advantage of.

* access to the EBI kubernetes cluster, which you can manage by running `kubectl` commands
* permission to claim for a fairly large storage capacity (10Ti)

## Configuration and setup

### Database server

Edit `secret.yaml` file to include the credentials to access the database. Make sure values are `base64` encoded and trimmed. After properly editing the secrets run

```bash
kubectl apply -f secret.yaml
kubectl apply -f pvc-postgres.yaml
kubectl apply -f postgres.yaml
```

### Helper pods

Start helper pods, clone codebase and initialize database schema:

```bash
kubectl apply -f pod-python.yaml
kubectl apply -f pod-rstudio.yaml
kubectl exec -it postgres-python -- git clone https://github.com/pkrisz5/vcf_database.git /x_scripts/repo
kubectl exec -it postgres-python -- python /x_scripts/repo/scripts/init_db.py
```

### Insert data in database by hand

```bash
kubectl exec -it postgres-rstudio -- Rscrip /x_scripts/repo/scripts/ebi_meta_script.r
kubectl exec -it postgres-rstudio -- Rscrip /x_scripts/repo/scripts/ebi_cov_script.r
kubectl exec -it postgres-rstudio -- Rscrip /x_scripts/repo/scripts/ebi_vcf_script.r
```

### Image preparation

It does not need to be run. Just a memory of how it was prepared.

```bash
docker build -t kooplex:rshiny-python .
docker tag kooplex:rshiny-python veo.vo.elte.hu:5000/k8plex:rshiny-python
docker push veo.vo.elte.hu:5000/k8plex:rshiny-python
```
