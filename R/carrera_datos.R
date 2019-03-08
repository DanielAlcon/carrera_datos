install.packages("data.table")
install.packages('curl')
library("data.table")
# leer datos desde url, pero faltaría concatenar todos los csvs en un solo dataframe
#data <- read.csv(url("https://console.cloud.google.com/storage/browser/securitas/natalidad000000000000.csv"))
dt <- fread("https://storage.googleapis.com/securitas/natalidad000000000000.csv")

