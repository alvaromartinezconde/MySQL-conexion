#Desactiva la notacion cientifica
options(scipen=999)

#setwd('E:/Business Intelligence')

#HTML
#library(xml2)
#library(rvest)

#Conexiones
library(odbc)
library(RODBC)
library(DBI)

#creamos conexion a MySQL
conMYSQL <- dbConnect(odbc::odbc(),
                      Driver = "MySQL ODBC 8.0 Unicode Driver",
                      Server = "127.0.0.1",
                      Port = 3306,
                      Database = "datamart_datamind",
                      UID = "a.martinez",
                      PWD = "Bienv3nid0"
)
#creamos conexion a MySQL
conMYSQL_log <- dbConnect(odbc::odbc(),
                          Driver = "MySQL ODBC 8.0 Unicode Driver",
                          Server = "127.0.0.1",
                          Port = 3306,
                          Database = "datamart_dm_logs",
                          UID = "a.martinez",
                          PWD = "Bienv3nid0"
)

############################################################
###########    funcion de carga de Logs en BBDD         ####
############################################################

registro_LOG<-function(hora_inicio, descripcion){
  Hora_fin = Sys.time()
  reg_log <- data.frame(hora_inicio, Hora_fin,descripcion)
  if (dbExistsTable(conn= conMYSQL_log, name = "tb_logs")==TRUE){
    historico <- dbReadTable(conn = conMYSQL_log, name = "tb_logs")
    reg_log <- rbind(historico,reg_log)
  }
  dbWriteTable(conn = conMYSQL_log, name = "tb_logs", reg_log,overwrite = TRUE)
  Hora_fin
}

############################################################
########### CARGA DE TABLA DE BBDD       ###################
############################################################

#registro Hora Inicio
hora_inicio <- Sys.time()

DATOS4 <- dbGetQuery(conMYSQL, 
                     "
SELECT * FROM datamart_datamind.tb_pobl_2015_4;
")

#registro en BBDD del LOG
Hora_fin <- registro_LOG(hora_inicio,"LECTURA DATOS DE TABLA 1==>Indicadores de renta media")
