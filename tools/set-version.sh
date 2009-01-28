#!/bin/bash

find . -name "*.py" | grep -v ".git" | xargs perl -i -p -e "s/trunk/$1/g"
