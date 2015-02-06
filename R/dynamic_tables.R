.CreateDataTable <- function(conn, config, tableName) {
  
  .message(paste0("Building table ", tableName, "."), config = config)
  
  query <- paste("CREATE TABLE IF NOT EXISTS", tableName,"
                 (ts timestamp with time zone NOT NULL,
                 seriesId text NOT NULL, 
                 familyId text, 
                 value numeric, 
                 paramcd text, 
                 validated integer, 
                 imported timestamp with time zone, 
                 updated timestamp with time zone, 
                 PRIMARY KEY(ts, seriesId) );")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste0("Successfully built table ", tableName, "."), config = config)
  
}

.CreateDataTableUpsertTrigger <- function(conn, config, tableName) {
  
  .message(paste0("Setting upsert trigger on table ", tableName, "."), config = config)
  
  query = paste0("CREATE OR REPLACE FUNCTION ", tableName,"_upsert() RETURNS TRIGGER AS $$
                BEGIN
                IF (SELECT COUNT(ts) FROM ", tableName, 
                 " WHERE ts = NEW.ts AND seriesid = NEW.seriesid) = 1 THEN
                UPDATE ", tableName, " SET 
                updated = NEW.updated,
                validated = NEW.validated,
                value = NEW.value
                WHERE ts = NEW.ts AND seriesid = NEW.seriesid;
                RETURN NULL;
                END IF;
                RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER ", tableName, "_data_merge 
                BEFORE INSERT ON ", tableName, 
                 " FOR EACH ROW EXECUTE PROCEDURE ", tableName, "_upsert();")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste0("Upsert trigger set for table ", tableName, "."), config = config)
}

.DropDataTableUpsertTrigger <- function(conn, config, tableName) {
  
  .message(paste0("Dropping upsert trigger on table ", tableName, "."), config = config)
  
  query = paste0("DROP TRIGGER ", tableName,"_data_merge;")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste0("Upsert trigger dropped for table ", tableName, "."), config = config)
}
