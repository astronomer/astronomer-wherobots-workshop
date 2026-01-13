#FROM quay.io/astronomer/astro-runtime:13.2.0

FROM astrocrpublic.azurecr.io/runtime:3.1-9

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER astro
