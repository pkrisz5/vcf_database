FROM veo.vo.elte.hu:5000/k8plex-ei-rstudio-basic

MAINTAINER Kooplex Team <kooplex@elte.hu>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
            python3-psycopg2 && \
    apt-get clean

