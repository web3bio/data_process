CREATE LOADING JOB Load_Ens FOR GRAPH SocialGraph {
  LOAD "/home/tigergraph/shared_data/import_graphs/farcaster/Identities.csv"
    TO VERTEX Identities VALUES($"primary_id", $"primary_id", $"platform", $"identity", $"update_time") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/farcaster/IdentitiesGraph.csv"
    TO VERTEX IdentitiesGraph VALUES($"primary_id", $"primary_id", $"updated_nanosecond") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/farcaster/PartOfIdentitiesGraph.csv"
    TO EDGE PartOfIdentitiesGraph VALUES ($"from", $"to") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";

  LOAD "/home/tigergraph/shared_data/import_graphs/farcaster/Hold.csv"
    TO EDGE Hold VALUES ($"from", $"to", $"source", $"level") USING SEPARATOR = "\t", EOL = "\n", HEADER = "true";
}