FROM r-base:3.6.0

RUN mkdir /app
WORKDIR /app

RUN R -e "install.packages('data.table', dependencies=TRUE, repos='http://cran.rstudio.com/')"

COPY . .