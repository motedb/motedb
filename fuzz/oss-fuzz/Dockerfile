# OSS-Fuzz project image for MoteDB.
# Copy this file to google/oss-fuzz/projects/motedb/Dockerfile.
FROM gcr.io/oss-fuzz-base/base-builder-rust

# git history is needed for cargo-fuzz to build the workspace cleanly.
RUN git clone --depth 1 https://github.com/motedb/motedb motedb

WORKDIR $SRC/motedb
COPY build.sh $SRC/build.sh

WORKDIR $SRC/motedb/fuzz
