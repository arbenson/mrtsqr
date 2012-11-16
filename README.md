MapReduce Matrix Computations
======

### David F. Gleich
### Paul G. Constantine
### Austin R. Benson

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

* B^T*A
* A*B for A tall-and-skinny and B small and square

Most codes are written in Python and use the NumPy library
for the numerical routines.  This introduces a mild-ineffiency
into the code.  Some C++ implementations are also provided in the 
`mrtsqr/cxx` directory.

The original paper by Constantine and Gleich is available at:

* Tall and skinny QR factorizations in MapReduce architectures [[pdf](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)]

This fork includes Austin's work on this project while at the
University of California, Berkeley.  Part of the work was completed in
part for Math 221: Advanced Matrix Computations (Prof. James Demmel) and
CS C267: Applications of Parallel Computers (Prof. James Demmel and Prof. Kathy Yelick).

Reports and posters can be found at the following places:

* [Math 221 report](http://arbenson.github.com/portfolio/Math221/AustinBenson-math221-report.pdf) (Fall 2011)
* [Math 221 poster](http://arbenson.github.com/portfolio/Math221/AustinBenson-math221-poster.pdf) (Fall 2011)
* [CS C267 report](http://arbenson.github.com/portfolio/CS267/AustinBenson-cs267-report.pdf) (Spring 2011)
* [CS C267 poster](http://arbenson.github.com/portfolio/CS267/AustinBenson-cs267-poster.pdf) (Spring 2011)

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

    # Run direct tsqr:
     python run_direct_tsqr.py --input=A_800M_10.bseq \
            --ncols=10 --svd=2 --schedule=100,100,100 \
            --hadoop=icme-hadoop1 --local_output=tsqr-tmp \
            --output=DIRTSQR_TESTING


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
        
    # Compute it's QR factorization
    
    dumbo start dumbo/tsqr.py -mat tsqr/verytiny.tmat -use_system_numpy
    
    # The -use_system_numpy option tells tsqr.py to 
    # use the numpy on the system.  On my cluster, the
    # compute nodes don't have numpy installed, so I ship
    # an egg with the streaming job to give them numpy.
    
    # Look at the R in the QR:
    
    dumbo cat tsqr/verytiny-qrr.mseq -hadoop $HADOOP_INSTALL

Overview
--------

* `dumbo/run_dirtsqr.py` - driver code for direct tsqr
* `dumbo/run_tsqr_ir.py` - driver code for direct tsqr with iterative refinement
* `dumbo/tsqr.py` - the indirect tsqr function for dumbo
* `cxx/tsqr.cc` - the tsqr code using C++
* `cxx/typedbytes.h` - the header file for the C++ typedbytes library
