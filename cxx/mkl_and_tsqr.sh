#!/bin/bash -l
module load mkl && chmod a+rwx tsqr
./tsqr $@

