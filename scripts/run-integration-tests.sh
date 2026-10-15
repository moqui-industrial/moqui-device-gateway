#!/usr/bin/env bash
set -euo pipefail

# Requires:
# - PostgreSQL initialized with the local test schema/seed or an equivalent Moqui DB.
# - ActiveMQ Artemis running with MQTT enabled.
#
# Standard Moqui Docker setup:
#   docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
#   docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
#
# The test expects the MQTT broker on:
#   tcp://localhost:1883
# with credentials:
#   artemis / artemis

cargo test --test integration_containers_test -- --nocapture
