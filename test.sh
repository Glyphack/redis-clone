#!/bin/bash
redis-cli xadd raspberry 0-1 foo bar
redis-cli xadd raspberry 0-2 foo bar
redis-cli xadd raspberry 0-3 foo bar
redis-cli xadd raspberry 0-4 foo bar
redis-cli xrange raspberry 0-2 0-4
