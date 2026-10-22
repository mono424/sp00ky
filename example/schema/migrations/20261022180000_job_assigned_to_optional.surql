-- Migration: job_assigned_to_optional
-- schema/src/outbox/api.surql made `assigned_to` optional (system jobs, such
-- as a `schedules:` fire or a durable workflow's run, belong to no thread),
-- but no migration carried that over, so the table still required it and
-- every job created without one was refused.
DEFINE FIELD OVERWRITE assigned_to ON TABLE job TYPE option<record<thread>>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;
