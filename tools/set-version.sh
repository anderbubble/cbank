#!/bin/bash

perl -i -p -e "s/trunk/$1/g" \
    setup.py source/packages/clusterbank/__init__.py rpm/clusterbank.spec
