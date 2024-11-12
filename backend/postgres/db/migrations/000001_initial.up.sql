DROP TABLE IF EXISTS instances;

CREATE TABLE instances (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  instance_id UUID NOT NULL,
  execution_id UUID NOT NULL,
  parent_instance_id UUID NULL,
  parent_execution_id UUID NULL,
  parent_schedule_event_id NUMERIC NULL,
  metadata BYTEA NULL,
  state INT NOT NULL,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at timestamptz NULL,
  locked_until timestamptz NULL,
  sticky_until timestamptz NULL,
  worker VARCHAR(64) NULL
);

CREATE UNIQUE INDEX idx_instances_instance_id_execution_id on instances (instance_id, execution_id);
CREATE INDEX idx_instances_locked_until_completed_at on instances (completed_at, locked_until, sticky_until, worker);
CREATE INDEX idx_instances_parent_instance_id_parent_execution_id ON instances (parent_instance_id, parent_execution_id);

DROP TABLE IF EXISTS pending_events;
CREATE TABLE pending_events (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  event_id UUID NOT NULL,
  sequence_id BIGSERIAL NOT NULL, -- Not used, but keep for now for query compat
  instance_id UUID NOT NULL,
  execution_id UUID NOT NULL,
  event_type INT NOT NULL,
  timestamp timestamptz NOT NULL,
  schedule_event_id BIGSERIAL NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at timestamptz NULL
);

CREATE INDEX idx_pending_events_inid_exid ON pending_events (instance_id, execution_id);
CREATE INDEX idx_pending_events_inid_exid_visible_at_schedule_event_id ON pending_events (instance_id, execution_id, visible_at, schedule_event_id);

DROP TABLE IF EXISTS history;
CREATE TABLE IF NOT EXISTS history (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  event_id UUID NOT NULL,
  sequence_id BIGSERIAL NOT NULL,
  instance_id UUID NOT NULL,
  execution_id UUID NOT NULL,
  event_type INT NOT NULL,
  timestamp timestamptz NOT NULL,
  schedule_event_id BIGSERIAL NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at timestamptz NULL
);

CREATE INDEX idx_history_instance_id_execution_id ON history (instance_id, execution_id);
CREATE INDEX idx_history_instance_id_execution_id_sequence_id ON history (instance_id, execution_id, sequence_id);

DROP TABLE IF EXISTS activities;
CREATE TABLE IF NOT EXISTS activities (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  activity_id UUID NOT NULL,
  instance_id UUID NOT NULL,
  execution_id UUID NOT NULL,
  event_type INT NOT NULL,
  timestamp timestamptz NOT NULL,
  schedule_event_id BIGSERIAL NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at timestamptz NULL,
  locked_until timestamptz NULL,
  worker VARCHAR(64) NULL
);

CREATE UNIQUE INDEX idx_activities_instance_id_execution_id_activity_id_worker ON activities (instance_id, execution_id, activity_id, worker);
CREATE INDEX idx_activities_locked_until on activities (locked_until);
