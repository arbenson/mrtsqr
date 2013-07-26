MapReduce Matrix Computations
======

### David F. Gleich, Paul G. Constantine, Austin R. Benson, James Demmel

The QR factorization is a standard matrix factorization used to solve
many problems.  Probably the most famous is linear regression:

    minimize || Ax - b ||,

where _A_ is an _m-by-n_ matrix, and _b_ is an _m-by-1_ vector.
When the number of rows of the matrix _A_ is much larger than
the number of columns, then _A_ is called a _tall-and-skinny_
matrix because of its shape.

The MapReduce codes implement several routines for computing the QR
factorization of a tall-and-skinny matrix.  We offer:

* Cholesky QR
* Direct TSQR (compute Q and R stably)
* Indirect TSQR (compute only R or also compute Q = AR^{-1})

We also implement Householder QR for performance comparisons only.  The other
algorithms are superior.  The underlying algorithm for Direct and Indirect
TSQR is due to Demmel et al. .  We also provide a few basic computations:

* B^T*A for B and A tall-and-skinny
* A*B for A tall-and-skinny and B small and square

Most codes are written in Python and use the NumPy library
for the numerical routines.  This introduces a mild-ineffiency
into the code.  Some C++ implementations are also provided in the 
`mrtsqr/cxx` directory.

The most recent work can be found in the preprint by Benson, Gleich, and Demmel at

* Direct QR factorizations for tall-and-skinny matrices in MapReduce architectures [[pdf](http://arxiv.org/abs/1301.1071)]

The original paper by Constantine and Gleich is available at:

* Tall and skinny QR factorizations in MapReduce architectures [[pdf](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)]



Synopsis
--------

Here, we detail the minimum possible steps required to get things
working.

### Setup

Ideally, there would be no setup.  However, to make things easier
at other stages, there are a few things you must do.

### Assumptions

* dumbo is installed and working
* numpy is installed and working
* hadoop is installed and working
* feathers is installed and working for Direct TSQR

### Example

    # Load all the paths.  You should update this for your setup.
    # This example only needs HADOOP_INSTALL set
    source setup_env.sh
    
    # Move a matrix into HDFS, properly formatted for our tools
    hadoop fs -mkdir tsqr
    hadoop fs -copyFromLocal data/verytiny.tmat tsqr/verytiny.tmat
    dumbo start dumbo/matrix2seqfile.py \
        -hadoop $HADOOP_INSTALL \
        -input tsqr/verytiny.tmat -output tsqr/verytiny.mseq
    
    # Look at the matrix in HDFS
    dumbo cat tsqr/verytiny.mseq -hadoop $HADOOP_INSTALL

    # Change directories
    cd dumbo

    # Compute Q and R stably and the singular values:
    python run_dirtsqr.py --input=tsqr/verytiny.mseq \
          --ncols=4 \
          --svd=1 \
          --hadoop=$HADOOP_INSTALL \
          --local_output=tsqr-tmp \
          --output=verytiny_qr

    # Look at the singular values
    dumbo cat verytiny_qr_2/Sigma/part-00000 -hadoop $HADOOP_INSTALL

    (depending on your install, you may or may not need the part-00000 extension)

Overview
--------

* `dumbo/run_dirtsqr.py` - driver code for direct tsqr
* `dumbo/run_tsqr_ir.py` - driver code for direct tsqr with iterative refinement
* `dumbo/tsqr.py` - the indirect tsqr function for dumbo
* `cxx/tsqr.cc` - the tsqr code using C++
* `cxx/typedbytes.h` - the header file for the C++ typedbytes library
