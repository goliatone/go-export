CREATE TABLE user_groups (
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 active BOOLEAN NOT NULL DEFAULT TRUE,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE user_group_memberships (
 group_id TEXT NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
 principal_id TEXT NOT NULL,
 active BOOLEAN NOT NULL DEFAULT TRUE,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(group_id,principal_id)
);
CREATE INDEX user_group_memberships_principal_idx ON user_group_memberships(principal_id,active,group_id);
