CREATE LOADING JOB Load_Ens FOR GRAPH SocialGraph {
  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/Identities.csv"
    TO VERTEX Identities VALUES($"primary_id", $"primary_id", $"platform", $"identity", $"update_time") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/IdentitiesGraph.csv"
    TO VERTEX IdentitiesGraph VALUES($"primary_id", $"primary_id", $"updated_nanosecond") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/PartOfIdentitiesGraph.csv"
    TO EDGE PartOfIdentitiesGraph VALUES ($"from", $"to") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";

  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/Hold.csv"
    TO EDGE Hold VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/Resolve.csv"
    TO EDGE Resolve VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
  LOAD "/home/tigergraph/shared_data/import_graphs/ensname/Reverse_Resolve.csv"
    TO EDGE Reverse_Resolve VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
}