require("data.table")
require(parallel)
library(data.table)

setwd("./data")

temp = list.files(pattern="natalidad*")
#race = read.csv("./race.csv", header = T, sep = ",")
#sex = read.csv("./sex.csv", header = T, sep = ",")
conv_factor_pounds_to_kgs = 2.2046

# detectamos número máximo de cores y seteamos para que use todos
setDTthreads(detectCores(all.tests = FALSE, logical = TRUE))

# creamos data table con el total de datos, eliminando columnas no necesarias
# columnas: source_year | state | is_male | child_race | weight_pounds
data_total = rbindlist(lapply(list.files(path=".", pattern="natalidad*"), data.table::fread , header = T, verbose = F, sep = ',', select = c("source_year", "state", "is_male", "child_race", "weight_pounds")))

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
avg_weight = data_total[source_year %between% c(1970,2010), .(avg_weight_pounds=mean(weight_pounds,na.rm = TRUE)), keyby = state]
avg_weight[, avg_weight_kgs := avg_weight_pounds * conv_factor_pounds_to_kgs, by=state]
avg_weight_kgs = avg_weight[, c('state', 'avg_weight_kgs'), with=FALSE]

# Unimos data tables
# TODO: usar función reduce para hacerlo todo de una vez
Merged = merge(b70,b80, by='state', all=TRUE)
Merged = merge(Merged,b90, by='state', all=TRUE)
Merged = merge(Merged,b00, by='state', all=TRUE)
Merged = merge(Merged,r70, by='state', all=TRUE)
Merged = merge(Merged,r80, by='state', all=TRUE)
Merged = merge(Merged,r90, by='state', all=TRUE)
Merged = merge(Merged,r00, by='state', all=TRUE)
Merged = merge(Merged,male, by='state', all=TRUE)
Merged = merge(Merged,female, by='state', all=TRUE)
Merged = merge(Merged,avg_weight_kgs, by='state', all=TRUE)

# Pasamos a csv
fwrite(Merged, "Merged.csv")
