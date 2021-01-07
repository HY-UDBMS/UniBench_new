library(plyr)
library(Jmisc)
library(BTYD)
library(dplyr)
rm(list = ls())

Unibench.import <- function(path) {
  # Read meta data
  metadata <- read.csv(path,header = FALSE)
  colnames(metadata) <- c("Brand", "PersonId", "Date", "price")
  metadata$PersonId<-as.character(metadata$PersonId)
  
  # Group by brand(V3) then apply CLVSC model to compute the value of customer
  SubData<-split(metadata,metadata$Brand)
  SubData
}

Unibench.ParameterEstimate<-function(data){
  data$Date <- strtrim(data$Date, 4)
  data$Date<-as.numeric(data$Date)
  time=split(data$Date,data$PersonId)
  money<-split(data$price,data$PersonId)
  
  avg<-lapply(money,mean)
  UniqueList=lapply(time,unique)
  Lengthlist<-lapply(UniqueList, length)
  MaxList<-lapply(time, max)
  
  list.recency<-do.call(rbind, MaxList) 
  list.frequency<-do.call(rbind, Lengthlist) 
  list.money<-do.call(rbind, avg) 
  
  ######################################
  #Estimate parameter of the bg/bb model
  rf.matrix<-cbind(list.frequency,list.recency)
  rf.matrix<-subset(rf.matrix,rf.matrix[,1]!=0)
  rf.matrix<-data.frame(rf.matrix)
  colnames(rf.matrix)<-c("x","t.x")
  
 # rf.matrix<-count(rf.matrix, vars = c("x","t.x"))
  rf.matrix <- ddply(rf.matrix, .(rf.matrix$x, rf.matrix$t.x), nrow)
  names(rf.matrix) <- c("x", "t.x", "custs")
  rf.matrix[,"t.x"]<-rf.matrix[,"t.x"]-2002
  rf.matrix=addCol(rf.matrix,7)
  colnames(rf.matrix)[4]<-"n.cal"
  rf.matrix[,"t.x"]=as.integer(rf.matrix[,"t.x"])
  rf.matrix[,"n.cal"]=as.integer(rf.matrix[,"n.cal"])
  rf.matrix[,"x"]=as.integer(rf.matrix[,"x"])
  params <- bgbb.EstimateParameters(rf.matrix,max.param.value =1000)
  
  
  ######################################
  # Estimate parameter according to the average spend and frequency
  params.spend=spend.EstimateParameters(list.money, list.frequency, par.start = c(1, 1, 1),max.param.value = 2000)

  # Estimate the parameters
  result=c(params,params.spend)  
  result
}

# function to write parameter list into list
fnlist <- function(x, fil){ z <- deparse(substitute(x))
  #cat(z, "\n", file=fil)
  nams=names(x) 
  for (i in seq_along(x) ){ 
    cat(nams[i], "\t",  x[[i]], "\n", file=fil, append=TRUE)
    }
}


# invoke the function to estimate the parameters
metadata<-Unibench.import("/Users/chzhang/test/CLVSC/CLVSC.csv")
result=lapply(metadata,Unibench.ParameterEstimate)
fnlist(result,"CLVSV_parameters.txt")



#lapply(result, write, "result.txt", append=TRUE)
#params<-lapply(result,function(x){round(x,4)})



# Check the convergence of model
################################
# LL <- bgbb.rf.matrix.LL(params, rf.matrix);
# p.matrix <- c(params, LL);
# for (i in 1:2){
#   params <- bgbb.EstimateParameters(rf.matrix, params);
#   LL <- bgbb.rf.matrix.LL(params, rf.matrix);
#   p.matrix.row <- c(params, LL);
#   p.matrix <- rbind(p.matrix, p.matrix.row);
# }
# colnames(p.matrix) <- c("alpha", "beta", "gamma", "delta", "LL");
# rownames(p.matrix) <- 1:3;
# p.matrix;


# # customer A
# n.cal = 6
# n.star = 10
# x = 0
# t.x = 0
# bgbb.ConditionalExpectedTransactions(params, n.cal,
#                                      n.star, x, t.x)
# # customer B
# x = 4
# t.x = 5
# bgbb.ConditionalExpectedTransactions(params, n.cal,
#                                      n.star, x, t.x)
