#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args)==0) {
  setwd("./data")
} else if (length(args)==1) {
  setwd(args[1])
}
print("cargando librerías")
require("data.table")
require("parallel")
library(data.table)
library(parallel)
print("creando lista de ficheros")
temp = list.files(pattern="natalidad*")
#race = read.csv("./race.csv", header = T, sep = ",")
#sex = read.csv("./sex.csv", header = T, sep = ",")
conv_factor_pounds_to_kgs = 2.2046

# detectamos número máximo de cores y seteamos para que use todos
print("seteando máximo número de núcleos a usar")
setDTthreads(detectCores(all.tests = FALSE, logical = TRUE))

# creamos data table con el total de datos, eliminando columnas no necesarias
# columnas: source_year | state | is_male | child_race | weight_pounds
print("generando data table total")
data_total = rbindlist(lapply(list.files(path=".", pattern="natalidad*"), data.table::fread , header = T, verbose = F, sep = ',', select = c("source_year", "state", "is_male", "child_race", "weight_pounds")))

# Total nacimientos por décadas y estado
print("generando columnas de nacimientos por décadas y estado")
b70 = setnames(data_total[source_year %between% c(1970,1979), .N, keyby = state],"N","b70")
print("generada década de los 70")
b80 = setnames(data_total[source_year %between% c(1980,1989), .N, keyby = state],"N","b80")
print("generada década de los 80")
b90 = setnames(data_total[source_year %between% c(1990,1999), .N, keyby = state],"N","b90")
print("generada década de los 90")
b00 = setnames(data_total[source_year %between% c(2000,2010), .N, keyby = state],"N","b00")
print("generada década de los 00")

# Raza con mayor nacimiento por década y estado
print("generando columnas de razas con mayores nacimientos por décadas y estado")
r70 = data_total[source_year %between% c(1970,1979), .(r70=max(child_race)), keyby = state]
print("generada década de los 70")
r80 = data_total[source_year %between% c(1980,1989), .(r80=max(child_race)), keyby = state]
print("generada década de los 80")
r90 = data_total[source_year %between% c(1990,1999), .(r90=max(child_race)), keyby = state]
print("generada década de los 90")
r00 = data_total[source_year %between% c(2000,2010), .(r00=max(child_race)), keyby = state]
print("generada década de los 00")

# Total nacimientos por sexo y estado
print("generando columnas de sexo por estado")
male = data_total[source_year %between% c(1970,2010), .(male=sum(is_male==TRUE)), keyby = state]
print("generado hombres")
female = data_total[source_year %between% c(1970,2010), .(female=sum(is_male==FALSE)), keyby = state]
print("generado mujeres")

# Pesos medios por estado
print("generando columnas de peso medio")
avg_weight = data_total[source_year %between% c(1970,2010), .(avg_weight_pounds=mean(weight_pounds,na.rm = TRUE)), keyby = state]
print("generado peso medio")
avg_weight[, avg_weight_kgs := avg_weight_pounds * conv_factor_pounds_to_kgs, by=state]
avg_weight_kgs = avg_weight[, c('state', 'avg_weight_kgs'), with=FALSE]
print("generado peso medio en kgs")

# Unimos data tables
# TODO: usar función reduce para hacerlo todo de una vez
print("generado data table final uniendo los anteriores")
print("uniendo b70 y b80")
Merged = merge(b70,b80, by='state', all=TRUE)
print("uniendo b70,b80 y b90")
Merged = merge(Merged,b90, by='state', all=TRUE)
print("uniendo b70,b80,b90 y b00")
Merged = merge(Merged,b00, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00 y r70")
Merged = merge(Merged,r70, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70 y r80")
Merged = merge(Merged,r80, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70,r80 y r90")
Merged = merge(Merged,r90, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70,r80,r90 y r00")
Merged = merge(Merged,r00, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70,r80,r90,r00 y male")
Merged = merge(Merged,male, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70,r80,r90,r00,male y female")
Merged = merge(Merged,female, by='state', all=TRUE)
print("uniendo b70,b80,b90,b00,r70,r80,r90,r00,male,female y avg_weight_kgs")
Merged = merge(Merged,avg_weight_kgs, by='state', all=TRUE)
print("data table final generado...")

# Pasamos a csv
print("escribiendo csv")
fwrite(Merged, "Merged.csv")
print("csv escrito, saliendo...")
