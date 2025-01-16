#!/bin/bash
exec "/opt/kafka/bin/connect-standalone.sh" "/opt/kafka/config/connector.properties" "/opt/kafka/config/source-connector.properties"