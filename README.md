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

