#!/bin/bash

. `dirname $0`/grid.env

if [ "x$PYTHON" == "x" ]; then
  PYTHON=python
fi

exec $PYTHON "$(dirname $0)/gridwatch.py" $@
