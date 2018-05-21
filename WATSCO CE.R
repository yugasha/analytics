
library (RPostgreSQL)
library("googlesheets")
suppressMessages(library("dplyr"))

drv <- dbDriver("PostgreSQL")

cons <- dbListConnections(drv)
for(conn in cons)
  +     +  dbDisconnect(conn)

conn <- dbConnect(drv, host="close-data.coce7zxnz6dr.us-east-2.redshift.amazonaws.com", 
                  port="5439",
                  dbname="dev", 
                  user="close_user", 
                  password="KeDJ9gWY-yh55A^f")

sql_BDcloseData <- dbGetQuery(conn, 
                              "SELECT DISTINCT
                              l.display_name as leadname
                              ,l.id
                              ,l.\"custom.lcf_po0saqxzhhowf9my3kbd3w4z2hs2sfivo8bk4p2nxco__bigint\" as \"Organization \"
                              ,l.status_label as status
                              ,l.date_created - interval '7 hours' as Created
                              ,l.\"custom__2-6 Re-engaged Lead Status\" as \"Re-engaged Date\"
                              ,l.\"custom__1-6: housecall category\" as Vertical
                              ,l.\"custom__1-1 rsdr\" as \"1-1 RSDR\"
                              ,l.\"custom__1-2: sdr\" as SDR
                              ,l.\"custom__1-3: es\" as ES
                              ,l.\"custom__1-4: marketing channel\" as \"1-10 Marketing Channel\"
                              ,l.\"custom__1-7: demo booked on\" - interval '7 hours' as \"Demo Booked\"
                              ,l.\"custom__1-70: demo booked for\" - interval '7 hours' as \"Demo Booked\"
                              ,l.\"custom__1-8: demo attended\" - interval '7 hours' as \"Demo Attended\"
                              ,l.\"custom__3-0 bql partner\" as \"3-0 BQL Partner\"
                              ,l.\"custom__most recent enrollment date\" - interval '7 hours' as \"date (most recent enrollment)\"
                              ,Act.calls
                              ,Act.firstcommunicationdate
                              ,o.status_label as opportunity
                              FROM closeio_housecallpro.leads as l
                              LEFT JOIN closeio_housecallpro.leads__contacts as c 
                              on l.id=c.lead_id
                              LEFT JOIN
                              (select
                              count(distinct a.id) as calls, a.lead_id,min(a.date_created - interval '7 hours') as firstcommunicationdate
                              from closeio_housecallpro.activities as a
                              where a._type='Call' AND a.direction in ('outbound','outgoing')
                              group by a.lead_id)as Act on l.id=Act.lead_id
                              LEFT JOIN closeio_housecallpro.leads__opportunities as o on l.id=o.lead_id
                              WHERE l.\"custom__1-4: marketing channel\"='ServiceChannel'
                              ")

#View(sql_BDcloseData)
sql_BDcloseData$updated <- Sys.Date()

setwd("~/Documents")
write.csv(sql_BDcloseData, file = "ServiceChannel.csv", row.names=FALSE)
FFC_GS <- gs_upload(sheet_title  = "ServiceChannel Leads", "ServiceChannel.csv", overwrite = TRUE)