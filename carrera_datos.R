start_time <- Sys.time()
require("data.table")
require(parallel)
#library(data.table)

setwd("/data")

temp = list.files(pattern="natalidad*")
# race = read.csv("./race.csv", header = T, sep = ",")
# sex = read.csv("./sex.csv", header = T, sep = ",")
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
r70 = data_total[source_year %between% c(1970,1979), .N, keyby = .(state,child_race)]
r70 = r70[, .SD[which.max(N)][,.(child_race)], by = state]
r80 = data_total[source_year %between% c(1980,1989),  .N, keyby = .(state,child_race)]
r80 = r80[, .SD[which.max(N)][,.(child_race)], by = state]
r90 = data_total[source_year %between% c(1990,1999),  .N, keyby = .(state,child_race)]
r90 = r90[, .SD[which.max(N)][,.(child_race)], by = state]
r00 = data_total[source_year %between% c(2000,2010),  .N, keyby = .(state,child_race)]
r00 = r00[, .SD[which.max(N)][,.(child_race)], by = state]

# Total nacimientos por sexo y estado
male = data_total[source_year %between% c(1970,2010), .(male=sum(is_male==TRUE)), keyby = state]
female = data_total[source_year %between% c(1970,2010), .(female=sum(is_male==FALSE)), keyby = state]

# Pesos medios por estado
avg_weight = data_total[source_year %between% c(1970,2010), .(avg_weight_pounds=mean(weight_pounds,na.rm = TRUE)), keyby = state]
avg_weight[, avg_weight_kgs := avg_weight_pounds / conv_factor_pounds_to_kgs, by=state]
avg_weight_kgs = avg_weight[, c('state', 'avg_weight_kgs'), with=FALSE]

# Unimos data tables
merged = b70[b80, on = .(state = state)] 
merged = merged[b90, on = .(state = state)]
merged = merged[b00, on = .(state = state)]
merged = setnames(merged[r70, on = .(state = state)],"child_race", "r70")
merged = setnames(merged[r80, on = .(state = state)],"child_race", "r80")
merged = setnames(merged[r90, on = .(state = state)],"child_race", "r90")
merged = setnames(merged[r00, on = .(state = state)],"child_race", "r00")
merged = merged[male, on = .(state = state)]
merged = merged[female, on = .(state = state)]
merged = merged[avg_weight_kgs, on = .(state = state)]

# modificar raza para coger valor 
merged[merged == 1] <- as.character("White")
merged[merged == 2] <- as.character("Black")
merged[merged == 3] <- as.character("American Indian")
merged[merged == 4] <- as.character("Chinese")
merged[merged == 5] <- as.character("Japanese")
merged[merged == 6] <- as.character("Hawaiian")
merged[merged == 7] <- as.character("Filipino")
merged[merged == 9] <- as.character("Unknown/Other")
merged[merged == 2] <- as.character("Black")
merged[merged == 9] <- as.character("Unknown/Other")
merged[merged == 18] <- as.character("Asian Indian")
merged[merged == 9] <- as.character("Unknown/Other")
merged[merged == 28] <- as.character("Korean")
merged[merged == 39] <- as.character("Samoan")
merged[merged == 48] <- as.character("Vietnamese")

# Pasamos a csv
fwrite(merged, "final.csv")

end_time <- Sys.time()

end_time - start_time
#print(timing)

