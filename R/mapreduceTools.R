library("ff")
mapper1 <- function(pdf){
  pdf = as.data.frame(table(pdf$Month))
  colnames(pdf) <- c("Month", "Freq")
  pdf
}
hasher1 <- function(pdf, nr){
  pdf$rbucket <- 1
  pdf
}
reducer1 <- function(pdf){
  pdf = aggregate(Freq ~ Month, data=pdf, sum)
  pdf
}

#longer solution
#bock.size = 200 mb
#' Apply complete mapreduce algorithm to dataset provided in the form of 
#' a filename of ffdf dataframe. Function parameters mapper, reducer and hasher 
#' constitutes the mapreduce algorithm to be applied  
#'
#' @param a ffdf object (optional: either a or filename must be provided)
#' @param filename large csv file's path (optional: either a or filename must be provided)
#' @param mapper mapper function to be used by mapreduce flow
#' @param reducer reducer function to be used by mapreduce flow
#' @param hasher hasher function, which decides the reduce bucket to be used
#' for a data row
#' @param nr number of reduce buckets to be used by mapreduce flow, defaults to 1
#' @param block.size approximate data chunk size in bytes, defaults to 200000000 or 200 mb
#' @name mapreduce
#' @rdname mapreduce
#' @export
#' @examples
#' one <- mtcars[1:10, ]
#' two <- mtcars[11:32, ]
#'
#' rbind_list(one, two)
#' rbind_all(list(one, two))
mapreduce <- function(a=NULL, filename, mapper, reducer, hasher, nr=1, block.size = 200000000){
  if(a==NULL){
    a <- read.csv.ffdf(file=filename, header=TRUE, VERBOSE=TRUE, first.rows=1000000, next.rows=1000000, colClasses=NA)
  }
  #a <- read.csv.ffdf(file="big.csv", header=TRUE, VERBOSE=TRUE, first.rows=1000000, next.rows=1000000, colClasses=NA)
  totalrows = dim(a)[1]
  row.size = as.integer(object.size(a[1:10000,])) / 10000
  
  rows.block = ceiling(block.size/row.size)
  nmaps = floor(totalrows/rows.block)
  #nmaps = 10
  #nmaps = number of maps - 1
  
  for(i in (0:nmaps)){
    if(i==nmaps){
      #df = a[(i*rows.block+1) : ((i+1)*rows.block),]
      df = a[(i*rows.block+1) : totalrows,]
    }
    else{
      df = a[(i*rows.block+1) : ((i+1)*rows.block),]
    }
    
    ##parameter a, block.size, nr, hasher,mapper, reducer
    # can be parameters first.rows, next.rows
    df = hasher(mapper(df), nr)
    mappedlist = split(df, df$rbucket)
    
    #bucket names or rbucket field should be 1 to nr
    rbucketnames = names(mappedlist)
    for(j in 1:length(mappedlist)){
      write.csv(mappedlist[[j]], paste0("R", rbucketnames[j],"M",i+1,".csv"))
    }
    rm(df)
  }
  
  for(i in 1:nr){
    rbucketname = as.character(i)
    for(j in 1:nmaps+1){
      fname = paste0("R", rbucketname, "M", j,".csv")
      
      if(file.exists(fname)){
        if(exists("rdf")){
          rdf = rbind(rdf, read.csv(fname))
        }
        else{
          rdf = read.csv(fname)
        }
      }
    }
    if(exists("rdf")){
      finalrdf = reducer(rdf)
      write.csv(finalrdf, paste0("R",rbucketname,".csv"))
      rm(rdf)
      rm(finalrdf)
    }  
  }
  
}

#global parameters nr, nmaps
#map parameters a #(ff dataframe), hasher, mapper
#reduce parameters reducer

#' Apply only mapping and hashing part of mapreduce flow to dataset provided in the form of 
#' a filename of ffdf dataframe. This function will generate files 
#' which will be used by reducer flow
#'
#' @param a ffdf object (optional: either a or filename must be provided)
#' @param filename large csv file's path (optional: either a or filename must be provided)
#' @param mapper mapper function to be used by mapreduce flow
#' @param hasher hasher function, which decides the reduce bucket to be used
#' for a data row
#' @param nr number of reduce buckets to be used by mapreduce flow, defaults to 1
#' @param block.size approximate data chunk size in bytes, defaults to 200000000 or 200 mb
#' @name mapreduce
#' @rdname mapreduce
#' @export
#' @examples
#' one <- mtcars[1:10, ]
#' two <- mtcars[11:32, ]
#'
#' rbind_list(one, two)
#' rbind_all(list(one, two))
refactored_map <- function(a=NULL, filename="big.csv", mapper=mapper1, hasher=hasher1, nr=1, block.size = 200000000){
  if(a==NULL){
    a <- read.csv.ffdf(file=filename, header=TRUE, VERBOSE=TRUE, first.rows=1000000, next.rows=1000000, colClasses=NA)
  }
  totalrows = dim(a)[1]
  row.size = as.integer(object.size(a[1:10000,])) / 10000
  
  rows.block = ceiling(block.size/row.size)
  nmaps = floor(totalrows/rows.block)
  for(i in (0:nmaps)){
    if(i==nmaps){
      #df = a[(i*rows.block+1) : ((i+1)*rows.block),]
      df = a[(i*rows.block+1) : totalrows,]
    }
    else{
      df = a[(i*rows.block+1) : ((i+1)*rows.block),]
    }
    
    ##parameter a, block.size, nr, hasher,mapper, reducer
    # can be parameters first.rows, next.rows
    df = hasher(mapper(df), nr)
    mappedlist = split(df, df$rbucket)
    
    #bucket names or rbucket field should be 1 to nr
    rbucketnames = names(mappedlist)
    for(j in 1:length(mappedlist)){
      write.csv(mappedlist[[j]], paste0("R", rbucketnames[j],"M",i+1,".csv"))
    }
    rm(df)
  }
  nmaps
}

#' Apply reduce flow to the output of the mapping part
#' of mapreduce flow. This function will generate output files
#'
#' @param a ffdf object (optional: either a or filename must be provided)
#' @param filename large csv file's path (optional: either a or filename must be provided)
#' @param reducer reducer function to be used by mapreduce flow
#' @param nmaps: number of maps or data chucks produced by the map task function#' @param nr number of reduce buckets to be used by mapreduce flow, defaults to 1
#' @param block.size approximate data chunk size in bytes, defaults to 200000000 or 200 mb
#' @name mapreduce
#' @rdname mapreduce
#' @export
#' @examples
#' one <- mtcars[1:10, ]
#' two <- mtcars[11:32, ]
#'
#' rbind_list(one, two)
#' rbind_all(list(one, two))
refactored_reduce <- function(nr, nmaps, reducer=reducer1){
  for(i in 1:nr){
    rbucketname = as.character(i)
    for(j in 1:nmaps+1){
      fname = paste0("R", rbucketname, "M", j,".csv")
      
      if(file.exists(fname)){
        if(exists("rdf")){
          rdf = rbind(rdf, read.csv(fname))
        }
        else{
          rdf = read.csv(fname)
        }
      }
    }
    if(exists("rdf")){
      finalrdf = reducer(rdf)
      write.csv(finalrdf, paste0("R",rbucketname,".csv"))
      rm(rdf)
      rm(finalrdf)
    }  
  }
}



