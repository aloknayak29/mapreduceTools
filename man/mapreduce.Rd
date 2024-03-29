% Generated by roxygen2 (4.0.2): do not edit by hand
\name{mapreduce}
\alias{mapreduce}
\alias{refactored_map}
\alias{refactored_reduce}
\title{Apply complete mapreduce algorithm to dataset provided in the form of
a filename of ffdf dataframe. Function parameters mapper, reducer and hasher
constitutes the mapreduce algorithm to be applied}
\usage{
mapreduce(a = NULL, filename, mapper, reducer, hasher, nr = 1,
  block.size = 2e+08)

refactored_map(a = NULL, filename = "big.csv", mapper = mapper1,
  hasher = hasher1, nr = 1, block.size = 2e+08)

refactored_reduce(nr, nmaps, reducer = reducer1)
}
\arguments{
\item{a}{ffdf object (optional: either a or filename must be provided)}

\item{filename}{large csv file's path (optional: either a or filename must be provided)}

\item{mapper}{mapper function to be used by mapreduce flow}

\item{reducer}{reducer function to be used by mapreduce flow}

\item{hasher}{hasher function, which decides the reduce bucket to be used
for a data row}

\item{nr}{number of reduce buckets to be used by mapreduce flow, defaults to 1}

\item{block.size}{approximate data chunk size in bytes, defaults to 200000000 or 200 mb}

\item{a}{ffdf object (optional: either a or filename must be provided)}

\item{filename}{large csv file's path (optional: either a or filename must be provided)}

\item{mapper}{mapper function to be used by mapreduce flow}

\item{hasher}{hasher function, which decides the reduce bucket to be used
for a data row}

\item{nr}{number of reduce buckets to be used by mapreduce flow, defaults to 1}

\item{block.size}{approximate data chunk size in bytes, defaults to 200000000 or 200 mb}

\item{a}{ffdf object (optional: either a or filename must be provided)}

\item{filename}{large csv file's path (optional: either a or filename must be provided)}

\item{reducer}{reducer function to be used by mapreduce flow}

\item{nmaps:}{number of maps or data chucks produced by the map task function#'}

\item{nr}{number of reduce buckets to be used by mapreduce flow, defaults to 1}

\item{block.size}{approximate data chunk size in bytes, defaults to 200000000 or 200 mb}
}
\description{
Apply complete mapreduce algorithm to dataset provided in the form of
a filename of ffdf dataframe. Function parameters mapper, reducer and hasher
constitutes the mapreduce algorithm to be applied

Apply only mapping and hashing part of mapreduce flow to dataset provided in the form of
a filename of ffdf dataframe. This function will generate files
which will be used by reducer flow

Apply reduce flow to the output of the mapping part
of mapreduce flow. This function will generate output files
}
\examples{
one <- mtcars[1:10, ]
two <- mtcars[11:32, ]

rbind_list(one, two)
rbind_all(list(one, two))
one <- mtcars[1:10, ]
two <- mtcars[11:32, ]

rbind_list(one, two)
rbind_all(list(one, two))
one <- mtcars[1:10, ]
two <- mtcars[11:32, ]

rbind_list(one, two)
rbind_all(list(one, two))
}

