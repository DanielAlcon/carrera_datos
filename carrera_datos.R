# comando para descarga de ficheros completa en bash
# gsutil -m cp gs://securitas/*.csv data/
install.packages("data.table")
# install.packages('curl')
# library("data.table")
# leer datos desde url, pero faltaría concatenar todos los csvs en un solo dataframe
#data <- read.csv(url("https://console.cloud.google.com/storage/browser/securitas/natalidad000000000000.csv"))
# dt <- fread("https://storage.googleapis.com/securitas/natalidad000000000000.csv")
# install.packages("ff")
# install.packages("ffbase")
# library(ff)


#hacer benchmark de lo que tarda: system.time(comando)
# TODO: cambiar directorior de trabajo a donde se encuentren los ficheros
setwd("./data")
#create a vector of the files that I want to load
temp = list.files(pattern="natalidad*")
race = read.csv("./race.csv", header = T, sep = ",")
sex = read.csv("./sex.csv", header = T, sep = ",")
#leer solo las columnas necesarias
# columnas: source_year | state | is_male | child_race | weight_pounds

library(data.table)
# system.time(
data_total = rbindlist(lapply(list.files(path=".", pattern="natalidad*"), data.table::fread , header = T, verbose = T, sep = ',', select = c("source_year", "state", "is_male", "child_race", "weight_pounds")))

# Total nacimientos por décadas y estado
b70 = setnames(data_total[source_year %between% c(1970,1979), .N, keyby = state],"N","b70")
b80 = setnames(data_total[source_year %between% c(1980,1989), .N, keyby = state],"N","b80")
b90 = setnames(data_total[source_year %between% c(1990,1999), .N, keyby = state],"N","b90")
b00 = setnames(data_total[source_year %between% c(2000,2010), .N, keyby = state],"N","b00")

# Raza con mayor nacimiento por década y estado
r70 = data_total[source_year %between% c(1970,1979), .(r70=max(child_race)), keyby = state]
r80 = data_total[source_year %between% c(1980,1989), .(r80=max(child_race)), keyby = state]
r90 = data_total[source_year %between% c(1990,1999), .(r90=max(child_race)), keyby = state]
r00 = data_total[source_year %between% c(2000,2010), .(r00=max(child_race)), keyby = state]

# Total nacimientos por sexo y estado
male = data_total[source_year %between% c(1970,2010), .(male=sum(is_male==TRUE)), keyby = state]
female = data_total[source_year %between% c(1970,2010), .(female=sum(is_male==FALSE)), keyby = state]

# Pesos medios por estado
Weight: peso medio en kilos de todos los niños nacidos en ese estado desde el 70 al 2010 (float)
remove NA rows
weight_no_NA = data_total[source_year %between% c(1970,2010), .(weight_no_NA=subset(data_total$weight_pounds, data_total$weight_pounds != is.na(data_total$weight_pounds))), keyby = 
                            state]
avg_weight = data_total[source_year %between% c(1970,2010), .(avg_weight=mean(weight_pounds!='NA')), keyby = state]

tabla_final = Unir columnas en un data.table para pasar a csv
dt = data.table(B70,B80[,'B80'], etc..)
# Guardar en CSV
fwrite(tabla_final, "tabla_final.csv")
# conteo de nacimientos sin agrupar por estado, usando funciones core de R
count_births = aggregate(cbind(count = weight_pounds) ~ state, 
                         data = data, 
                         FUN = function(x){NROW(x)})
user  system elapsed 
0.805   0.172   0.990

###########################################################################################
#                                                                                         #
#                                       librería ff                                       #
#                                                                                         #
###########################################################################################
library(ff)
library(ffbase)
system.time(read.csv.ffdf(file = '../data/natalidad000000000000.csv', header = T))
# user  system elapsed 
# 9.634   1.199  11.128



#create the first ffdf object for i = 1, this is necessary to establish the ff dataframe to append the rest
for (i in 1)
  ruta=paste("../data/",temp[i],sep="")
  print(ruta)
  mydata <- read.csv.ffdf(file=ruta, header=TRUE, VERBOSE=TRUE)

#loop through the remaining objects
for (i in 2:length(temp))
  ruta=paste("../data/",temp[i],sep="")
  print(ruta)
  mydata <- read.csv.ffdf(x = mydata, file=ruta, header=TRUE, VERBOSE=TRUE)


for (i in temp) {
  ruta=paste("../data/",i,sep="")
  print(ruta)
  mydata <- read.csv.ffdf(file=ruta, header=TRUE, VERBOSE=TRUE)
}


