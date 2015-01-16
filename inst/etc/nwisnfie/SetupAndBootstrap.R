library(nwisnfie)

config <- LoadConfiguration()

PopulateStaticTables(config)

BuildDynamicTables(config)

Bootstrap(config)