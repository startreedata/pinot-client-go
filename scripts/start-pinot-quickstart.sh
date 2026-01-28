#!/bin/bash

# Set the Pinot version
if [ -z "${PINOT_VERSION}" ]; then
  echo "PINOT_VERSION is not set. Using default version 1.4.0"
  PINOT_VERSION="1.4.0"
fi

# Set the download URL
DOWNLOAD_URL="https://archive.apache.org/dist/pinot/apache-pinot-${PINOT_VERSION}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

# Set the destination directory
if [ -z "${PINOT_HOME}" ]; then
  echo "PINOT_HOME is not set. Using default directory /tmp/pinot"
  PINOT_HOME="/tmp/pinot"
fi

# Set the broker port
if [ -z "${BROKER_PORT_FORWARD}" ]; then
  echo "BROKER_PORT_FORWARD is not set. Using default port 8000"
  BROKER_PORT_FORWARD="8000"
fi

# Set the broker gRPC port
if [ -z "${BROKER_GRPC_PORT_FORWARD}" ]; then
  echo "BROKER_GRPC_PORT_FORWARD is not set. Using default port 8010"
  BROKER_GRPC_PORT_FORWARD="8010"
fi

# Create the destination directory
mkdir -p "${PINOT_HOME}"

# Write quickstart config overrides (Pinot 1.4 does not support -brokerGrpcPort)
QUICKSTART_CONFIG_FILE="${PINOT_HOME}/quickstart-config.properties"
cat > "${QUICKSTART_CONFIG_FILE}" <<EOF
pinot.broker.grpc.port=${BROKER_GRPC_PORT_FORWARD}
EOF

cat "${QUICKSTART_CONFIG_FILE}"

# Check if the directory exists
if [ -d "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin" ]; then
    echo "Pinot package already exists in ${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin"
else
    TAR_PATH="${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"
    CHECKSUM_URL="${DOWNLOAD_URL}.sha512"

    if [ -f "${TAR_PATH}" ]; then
        REMOTE_SHA=$(curl -fsSL "${CHECKSUM_URL}" | cut -d ' ' -f 1)
        LOCAL_SHA=$(shasum -a 512 "${TAR_PATH}" | cut -d ' ' -f 1)
        if [ "${REMOTE_SHA}" != "${LOCAL_SHA}" ]; then
            echo "Checksum mismatch for ${TAR_PATH}. Re-downloading."
            rm "${TAR_PATH}"
        fi
    fi

    if [ ! -f "${TAR_PATH}" ]; then
        # Download the Pinot package
        curl -L "${DOWNLOAD_URL}" -o "${TAR_PATH}"
    fi

    # Extract the downloaded package
    tar -xzf "${TAR_PATH}" -C "${PINOT_HOME}"

    # Remove the downloaded package
    rm "${TAR_PATH}"
fi


# Start the Pinot cluster
# NOTE: QuickStart type is explicitly set to BATCH. This differs from MULTI_STAGE
# QuickStart and changes how queries are executed and how the cluster behaves.
# If changing this type (e.g., to MULTI_STAGE), document the rationale and
# behavioral impact in the PR description and/or in this script.
${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin/bin/pinot-admin.sh QuickStart -type BATCH -configFile "${QUICKSTART_CONFIG_FILE}" &
PID=$!

# Print the JVM settings
jps -lvm

### ---------------------------------------------------------------------------
### Ensure Pinot cluster started correctly.
### ---------------------------------------------------------------------------

echo "Ensure Pinot cluster started correctly"

# Wait at most 10 minutes to reach the desired state
for i in $(seq 1 150)
do
  SUCCEED_TABLE=0
  for table in "airlineStats" "baseballStats" "dimBaseballTeams" "githubComplexTypeEvents" "githubEvents" "starbucksStores";
  do
    QUERY="select count(*) from ${table} limit 1"
    QUERY_REQUEST='curl -s -X POST -H '"'"'Accept: application/json'"'"' -d '"'"'{"sql": "'${QUERY}'"}'"'"' http://localhost:'${BROKER_PORT_FORWARD}'/query/sql'
    echo ${QUERY_REQUEST}
    QUERY_RES=`eval ${QUERY_REQUEST}`
    echo ${QUERY_RES}

    if [ $? -eq 0 ]; then
      COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
      if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
        SUCCEED_TABLE=$((SUCCEED_TABLE+1))
      fi
    fi
    echo "QUERY: ${QUERY}, QUERY_RES: ${QUERY_RES}"
  done
  echo "SUCCEED_TABLE: ${SUCCEED_TABLE}"
  if [ "${SUCCEED_TABLE}" -eq 6 ]; then
    break
  fi
  sleep 4
done

if [ "${SUCCEED_TABLE}" -lt 6 ]; then
  echo 'Quickstart failed: Cannot confirmed count-star result from quickstart table in 10 minutes'
  exit 1
fi
echo "Pinot cluster started correctly"
