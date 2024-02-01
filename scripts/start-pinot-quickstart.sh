#!/bin/bash

# Set the Pinot version
if [ -z "${PINOT_VERSION}" ]; then
  echo "PINOT_VERSION is not set. Using default version 1.0.0"
  PINOT_VERSION="1.0.0"
fi

# Set the download URL
DOWNLOAD_URL="https://archive.apache.org/dist/pinot/apache-pinot-${PINOT_VERSION}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

# Set the destination directory
if [ -z "${PINOT_HOME}" ]; then
  echo "PINOT_HOME is not set. Using default directory /tmp/pinot"
  PINOT_HOME="/tmp/pinot"
fi

# Create the destination directory
mkdir -p "${PINOT_HOME}"

# Download the Pinot package
curl -L "${DOWNLOAD_URL}" -o "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

# Extract the downloaded package
tar -xzf "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz" -C "${PINOT_HOME}"

# Remove the downloaded package
rm "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

# Start the Pinot cluster
${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin/bin/pinot-admin.sh QuickStart -type MULTI_STAGE &
PID=$!

# Print the JVM settings
jps -lvm

### ---------------------------------------------------------------------------
### Ensure Pinot cluster started correctly.
### ---------------------------------------------------------------------------

echo "Ensure Pinot cluster started correctly"

# Wait at most 5 minutes to reach the desired state
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
  sleep 2
done

if [ "${SUCCEED_TABLE}" -lt 6 ]; then
  echo 'Quickstart failed: Cannot confirmed count-star result from quickstart table in 5 minutes'
  exit 1
fi
echo "Pinot cluster started correctly"
