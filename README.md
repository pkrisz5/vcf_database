# vcf_database

The purpose of this project is to document how to set up the postgres database and report server hosted at ebi.

## Prerequisities

Certain services and resources need to be setup in advence and be operational to take advantage of.

* access to the EBI kubernetes cluster, which you can manage by running `kubectl` commands
* permission to claim for a fairly large storage capacity (10Ti)

_Note:_ given a cluster configuration file `ebi.config` the proper command line starts like `kubectl --kubeconfig ebi.config`

## Configuration and setup

### Create a separate namespace in the cluster

In this setup the namespace is called `kooplex-veo`. If you do not stick to it revise all manifest files in the `k8s` folder before applying them.

```bash
kubectl create namespace kooplex-veo
```

### Database server

Use the `secret.yaml` template file and edit it to include the credentials to access the database. Make sure values are `base64` encoded and trimmed. 

_Example:_ to encode a string you may run `echo -n -e i_want_to_encode_it | base64`.

```bash
kubectl apply -f secret.yaml
kubectl apply -f pvc-postgres.yaml
kubectl apply -f postgres.yaml
```

### Storage place for raw data files

Those files picked up by the database loader scripts are placed in a separate volume. Create it by issuing the following request.

```bash
kubectl apply -f pvc-raw.yaml
```

### A helper pod

Start a helper pod to clone codebase, initialize database schema and prepare the folder structure. 
Later on this pod can be used to visit log files and/or run scripts manually in case of an error not handled automagically.

```bash
kubectl apply -f pod-shell.yaml
kubectl exec -it postgres-shell -- git clone https://github.com/pkrisz5/vcf_database.git /mnt/repo
kubectl exec -it postgres-shell -- /mnt/repo/scripts/mkdir.sh
kubectl exec -it postgres-shell -- python3 /mnt/repo/scripts/init_db.py --init_db
kubectl exec -it postgres-shell -- python3 /mnt/repo/scripts/init_db.py --create_user
kubectl exec -it postgres-shell -- python3 /mnt/repo/scripts/operation.py init
```

### Populate data

#### One time data insertion

Currently the `lineage_def` table is filled with constant information.

```bash
kubectl exec -it postgres-shell -- Rscript /mnt/repo/scripts/lineage_def_script.R
```

#### Automatic data insertion

A cronjob takes care of inserting new data in the database. To start the cronjob run the following.

```bash
kubectl apply -f cron.yaml
```

#### Insert data in database by hand

TBA

### Start report server

Reports are served by an R-shiny server from the `app/` folder. Should just a subset of available reports be available edit the mount points as necessary in `report.yaml` then start the service.

```bash
kubectl apply -f report.yaml
```

# Image preparation

_Note:_ This part does not need to be run. It is just a memory of how the common image was built.

```bash
docker build -t kooplex:rshiny-python .
docker tag kooplex:rshiny-python veo.vo.elte.hu:5000/k8plex:rshiny-python
docker push veo.vo.elte.hu:5000/k8plex:rshiny-python
```
