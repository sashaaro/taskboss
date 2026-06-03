FROM postgres:18

ARG VERSION=0.1.0

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fsSL \
       "https://github.com/sashaaro/taskboss/releases/download/v${VERSION}/taskboss-${VERSION}-pg18-x86_64-unknown-linux-gnu.tar.gz" \
       | tar -xz -C / \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN echo "shared_preload_libraries = 'taskboss'" >> /usr/share/postgresql/postgresql.conf.sample
