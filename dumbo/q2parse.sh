#!/usr/bin/bash -v

rm -rf $1_tmp*

tr ",\n" ", " < $1 > $1_tmp1
tr ")" "\n" < $1_tmp1 > $1_tmp3
tr -d "]" < $1_tmp3 > $1_tmp4
tr -d "[" < $1_tmp4 > $1_tmp5

mv $1_tmp5 $1

rm -rf $1_tmp*

