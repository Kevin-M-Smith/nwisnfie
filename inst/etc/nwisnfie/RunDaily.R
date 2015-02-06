library(nwisnfie)

config <- LoadConfiguration()

SyncDB(config, period = "PT25H")

