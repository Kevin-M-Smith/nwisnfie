SuperTest <- function(config){
  TestClusterSettings(config)
  RunDBDiagnostics(config)
  RebuildStaticTables(config)
}