DROP TABLE IF EXISTS attributes;

CREATE TABLE attributes (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  event_id UUID NOT NULL,
  instance_id UUID NOT NULL,
  execution_id UUID NOT NULL,
  data BYTEA NOT NULL
);

CREATE UNIQUE INDEX idx_attributes_instance_id_execution_id_event_id on attributes (instance_id, execution_id, event_id);
CREATE INDEX idx_attributes_event_id on attributes (event_id); 

-- Move activity attributes to attributes table
INSERT INTO attributes (event_id, instance_id, execution_id, data) SELECT activity_id, instance_id, execution_id, attributes FROM activities ON CONFLICT DO NOTHING;
ALTER TABLE activities DROP COLUMN attributes;

-- Move history attributes to attributes table
INSERT INTO attributes (event_id, instance_id, execution_id, data) SELECT event_id, instance_id, execution_id, attributes FROM history ON CONFLICT DO NOTHING;
ALTER TABLE history DROP COLUMN attributes;

-- Move pending_events attributes to attributes table
INSERT INTO attributes (event_id, instance_id, execution_id, data) SELECT event_id, instance_id, execution_id, attributes FROM pending_events ON CONFLICT DO NOTHING;
ALTER TABLE pending_events DROP COLUMN attributes;
