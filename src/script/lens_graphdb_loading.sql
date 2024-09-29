CREATE LOADING JOB Load_Ens FOR GRAPH SocialGraph {
  LOAD "/home/tigergraph/shared_data/import_graphs/lens/Identities.csv"
    TO VERTEX Identities VALUES($"primary_id", $"primary_id", $"platform", $"identity", $"update_time") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/lens/IdentitiesGraph.csv"
    TO VERTEX IdentitiesGraph VALUES($"primary_id", $"primary_id", $"updated_nanosecond") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/lens/PartOfIdentitiesGraph.csv"
    TO EDGE PartOfIdentitiesGraph VALUES ($"from", $"to") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";

  LOAD "/home/tigergraph/shared_data/import_graphs/lens/Hold.csv"
    TO EDGE Hold VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
  LOAD "/home/tigergraph/shared_data/import_graphs/lens/Resolve.csv"
    TO EDGE Resolve VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
  LOAD "/home/tigergraph/shared_data/import_graphs/lens/Reverse_Resolve.csv"
    TO EDGE Reverse_Resolve VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
}