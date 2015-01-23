
library(nwisnfie)

config <- LoadConfiguration()

conn <- StartDBConnection(config)

query <- paste("select * from site_assets;")

site.assets <- RunQuery(conn = conn,
                        query = query,
                        config = config)

counts <- plyr::count(site.assets, "parm_cd")

descr <- subset(parameter_codes, select = c("parameter_cd", "parameter_nm", "parameter_units"))

counts <- merge(x = counts, 
                y = parameter_codes, 
                by.x = "parm_cd",
                by.y = "parameter_cd")

write.csv(counts, file = "nwis-iv-parameter-frequencies.csv")


query <- paste("select site_no, parm_cd from site_assets
               where 
               parm_cd = '00045' OR
               parm_cd = '72192' OR
               parm_cd = '99772' OR
               parm_cd = '00193';")

res <- RunQuery(conn = conn,
                query = query,
                config = config)

des <- merge(x = res,
             y = plyr::count(res, "site_no"),
             by = "site_no")

sub <- subset(des, freq == 1)
sub <- subset(sub, parm_cd != '00045')

sub2 <- subset(sub, parm_cd == '72192')
cat("The number of stations reporting precipitation only in cumulative inches (72192) is:", nrow(sub2))

sub3 <- subset(sub, parm_cd == '99772')
cat("The number of stations reporting precipitation only in mm (99772) is:", nrow(sub3))

sub4 <- subset(sub, parm_cd == '2609')
cat("The number of stations reporting precipitation as a total for period (00193) is:", nrow(sub4))