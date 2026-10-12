CREATE OR REPLACE FUNCTION public.runtime_guard_artifact_file()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM runtime_artifact_set
        WHERE set_id=CASE WHEN TG_OP='DELETE' THEN OLD.set_id ELSE NEW.set_id END FOR UPDATE;
    IF TG_OP='INSERT' AND current_state='STAGING' THEN RETURN NEW; END IF;
    IF TG_OP='DELETE' AND current_state='DELETING' THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'Artifact file mutation is not permitted in state %', current_state;
END $function$;

CREATE OR REPLACE FUNCTION public.runtime_guard_artifact_set()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.state IN ('SEALED','DELETING') AND
       ((to_jsonb(NEW)-'state') IS DISTINCT FROM (to_jsonb(OLD)-'state') OR
        NEW.state NOT IN (OLD.state,'DELETING')) THEN
        RAISE EXCEPTION 'Sealed artifact set is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $function$;
