#!/bin/bash

if ! which soda > /dev/null; then
    go install github.com/gobuffalo/pop/v6/soda@latest
fi
