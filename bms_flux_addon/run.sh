#!/usr/bin/env bash
# Plain shell run script. Supervisor runs tini as PID 1 via init: true, so
# signals reach bms-flux directly. The add-on never needs bashio/s6.
set -e

exec bms-flux run
