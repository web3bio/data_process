CREATE LOADING JOB Load_Clusters FOR GRAPH SocialGraph {
  LOAD "/home/tigergraph/shared_data/import_graphs/clusters/Identities.csv" TO VERTEX Identities VALUES($"primary_id", $"primary_id", $"platform", $"identity", $"update_time") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/clusters/IdentitiesGraph.csv" TO VERTEX IdentitiesGraph VALUES($"primary_id", $"primary_id", $"updated_nanosecond") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/clusters/PartOfIdentitiesGraph.csv" TO EDGE PartOfIdentitiesGraph VALUES($"from", $"to") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/clusters/Hold.csv" TO EDGE Hold VALUES($"from", $"to", $"source", $"level") USING SEPARATOR="\t", HEADER="true", EOL="\n";
  LOAD "/home/tigergraph/shared_data/import_graphs/clusters/Proof_Forward.csv" TO EDGE Proof_Forward VALUES($"from", $"to", $"source", $"level", $"record_id") USING SEPARATOR="\t", HEADER="true", EOL="\n";
}