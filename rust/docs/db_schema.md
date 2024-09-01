Table replica {
  id uuid [primary key]
  latest_commit_id uuid
}

Table commit {
  id uuid [primary key]
  version text
  ref_id bigint
  prev_commit_id uuid
}

Table object {
  id bigint [primary key]
  value text
}

Table ref {
  id bigint [primary key]
  left bigint
  right bigint
  object_ref bigint
}

Ref: commit.id < replica.latest_commit_id
Ref: ref.object_ref < object.id
Ref: commit.ref_id < ref.id
