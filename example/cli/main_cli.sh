#!/bin/bash

fwirl summarize test_graph

fwirl ls test_graph --assets --schedules --jobs

fwirl build test_graph

fwirl schedule test_graph test_build_schedule build '5 * * * *'

fwirl pause test_graph Reliable

fwirl refresh test_graph

fwirl unpause test_graph Reliable

fwirl pause test_graph test_build_schedule

fwirl ls test_graph --schedules --jobs

fwirl unpause test_graph test_build_schedule

fwirl ls test_graph --schedules --jobs

fwirl unschedule test_graph test_build_schedule

fwirl shutdown test_graph
