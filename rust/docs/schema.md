Table replica {
  id uuid [primary key]
  latest_commit_id uuid
}

Table commit {
  id uuid [primary key]
  version text
  object_hash bigint
  prev_commit_id uuid
}

Table object {
  hash bigint [primary key]
  value text
}

Ref: commit.id < replica.latest_commit_id
Ref: commit.object_hash < object.hash
