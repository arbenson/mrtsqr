MapReduce Matrix Computations (MRMC)
--------
Austin R. Benson, David F. Gleich, Paul G. Constantine, and James Demmel

This software provides a number of matrix computations for Hadoop Mapreduce,
using Python Hadoop streaming.  Among the computations, we focus on providing
a variety of methods for computing the QR factorization and the SVD.
The QR factorization is a standard matrix factorization used to solve many
problems.  Probably the most famous is linear regression:

    minimize || Ax - b ||,

where _A_ is an _m-by-n_ matrix, and _b_ is an _m-by-1_ vector.
When the number of rows of the matrix _A_ is much larger than
the number of columns (_m_ >> _n_), then _A_ is called a _tall-and-skinny_
matrix because of its shape.

The SVD is a small extension of QR when the matrix _A_ is tall-and-skinny.  If

     A = QR

and

     R = USV',

then the SVD of A is

     A = (QU)SV'.

Since _R_ is _n-by-n_, computing its SVD is cheap (O(n^3) operations).
The QR implementations we offer are:

* Compute just the _R_ factor, using TSQR or Cholesky QR
* Indirect TSQR (compute _R_ followed by _Q = AR^{-1}_).  This is an unstable computation of _Q_.
* Indirect TSQR + (pseudo-)Iterative Refinement.
* Direct TSQR.  This is a stable computation of _Q_.
* Householder QR.  This is for performance results only--the other algorithms are superior in MapReduce.

All of the analogous SVD computations are also available, with no additional cost in running time.
We also provide the following computations that may be useful:

* _B^T * A_ for tall-and-skinny matrices _A_ and _B_.
* _A^T * A_ for a tall-and-skinny matrix _A_.
* _A * B_ for tall-and-skinny _A_ and small _B_.

These codes are written using Python Hadoop streaming.  We use NumPy for local matrix computations
and Dumbo for managing the streaming.
Some C++ implementations are also provided in the `mrtsqr/cxx` directory.

The most recent work can be found in the following paper by Benson, Gleich, and Demmel.  Please cite 
the following paper if you use this software:

* [Direct QR factorizations for tall-and-skinny matrices in MapReduce architectures](http://dx.doi.org/10.1109/BigData.2013.6691583)

The original paper by Constantine and Gleich on MapReduce TSQR is:

* [Tall and skinny QR factorizations in MapReduce architectures](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)

The original work on the TSQR by Demmel et al. is:

* [Communication-optimal Parallel and Sequential QR and LU Factorizations](http://dx.doi.org/10.1137/080731992)

Setup
--------
This code requires the following software:

* Hadoop
* Python+NumPy
* [Dumbo](https://github.com/klbostee/dumbo/) (+ [Feathers](https://github.com/klbostee/feathers) for Direct TSQR)

Once everything is installed, run the small tests for MRTSQR:

     cd dumbo
     python run_tests.py all

R and singular values examples
--------
Here, we give a brief overview of the code and a small working example.
For this example, we need to set the environment variable HADOOP_INSTALL to point
to Hadoop on your system.  For example:

     HADOOP_INSTALL=/usr/lib/hadoop

Our first example shows how to compute the R factor and singular values of a small matrix:
    
    # Move a matrix into HDFS, properly formatted for our tools
    hadoop fs -mkdir tsqr
    hadoop fs -copyFromLocal data/verytiny.tmat tsqr/verytiny.tmat
    dumbo start dumbo/matrix2seqfile.py \
    -hadoop $HADOOP_INSTALL \
    -input tsqr/verytiny.tmat -output tsqr/verytiny.mseq

    # Look at the matrix in HFDS
    dumbo cat tsqr/verytiny.mseq -hadoop $HADOOP_INSTALL
    
    # Run TSQR
    dumbo start dumbo/tsqr.py -mat tsqr/verytiny.mseq -hadoop $HADOOP_INSTALL

    # Look at R in HDFS
    dumbo cat tsqr/verytiny-qrr.mseq -hadoop $HADOOP_INSTALL

    # Run TSQR with a different reduce schedule and output name
    dumbo start dumbo/tsqr.py -mat tsqr/verytiny.mseq -reduce_schedule 2,1 \
     -hadoop $HADOOP_INSTALL \
     -output verytiny-qrr-double-reduce.mseq

    # Look at R (should be the same, up to sign)
    dumbo cat verytiny-qrr-double-reduce.mseq -hadoop $HADOOP_INSTALL

    # Run TS-SVD to compute the singular values
    dumbo start dumbo/tssvd.py -mat tsqr/verytiny.mseq -hadoop $HADOOP_INSTALL

    # Look at the singular values
    dumbo cat tsqr/verytiny-svd.mseq -hadoop $HADOOP_INSTALL

Our second example shows how to stably compute the thin QR and SVD factorization of
the same small matrix.  For this example, Feathers needs to be installed and feathers.jar
on the Java classpath.

    # Change directories
    cd dumbo

    # Compute Q, R, and singular values stably:
    python run_dirtsqr.py --input=tsqr/verytiny.mseq \
          --ncols=4 \
          --svd=1 \
          --hadoop=$HADOOP_INSTALL \
          --local_output=tsqr-tmp \
          --output=verytiny_qr_svd

    # Look at R
    dumbo cat verytiny_qr_svd_2/R_final -hadoop $HADOOP_INSTALL

    # Look at the singular values
    dumbo cat verytiny_qr_svd_2/Sigma -hadoop $HADOOP_INSTALL
    
The matrix _Q_ is stored in `verytiny_qr_svd_3` on HDFS.  However, we store it in the compressed
TypedBytes string format (as opposed to TypedBytes list format) for efficiency.  This makes the output
of cat unreadable but computations using _Q_ faster.  We can make sure that _Q_ is orthogonal:

     dumbo start AtA.py -mat verytiny_qr_svd_3 \
          -output verytiny_QtQ.mseq -hadoop $HADOOP_INSTALL
     # Q^T * Q should be close to the identity matrix
     dumbo cat verytiny_QtQ.mseq -hadoop $HADOOP_INSTALL


Many QRs
--------
In this example, we look at many of the methods provided for computing the QR factorization.
We will use a slightly larger test matrix for this example.  The following code creates a
_10M-by-20_ matrix.

     hadoop fs -copyFromLocal data/Simple_10k.txt Simple_10k.txt
     dumbo start dumbo/GenBigExample.py -mat Simple_10k.txt \
        -output A_10M_20.bseq -hadoop $HADOOP_INSTALL

Now, we compute the QR factorization of this matrix using several different methods:

     cd dumbo
     # TSQR + AR^{-1}.
     python run_tsqr_arinv.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --schedule=20,1 --output=ARINV
     # Cholesky QR + AR^{-1}.  Option use_cholesky specifies number of columns.
     python run_tsqr_arinv.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --schedule=20,1 --output=ARINV_CHOL --use_cholesky=20
     
     # Direct TSQR.
     python run_dirtsqr.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --ncols=20 --svd=0 --schedule=20,20,20 --output=DIRTSQR
     # Recursive Direct TSQR.
     python run_rec_dirtsqr.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --ncols=20 --output=REC_DIRTSQR

     # Indirect TSQR + iterative refinement.
     python run_tsqr_ir.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --schedule=20,1 --output=TSQR_IR
     # Indirect TSQR + pseudo-iterative refinement.
     python run_tsqr_ir.py --hadoop=$HADOOP_INSTALL --input=A_10M_20.bseq \
          --schedule=20,1 --output=TSQR_PIR --use_pseudo=1


Contact
--------

For questions, suggestions, bug reports, and contributions, please email Austin Benson: arbenson AT stanford DOT edu.
