#!/bin/bash -l
export LDPATH="/usr/lib64/atlas:$LDPATH"
export LDPATH="/usr/lib64/:$LDPATH"
export PATH="/usr/lib64/:$PATH"
chmod a+rwx tsqr
./tsqr $@

