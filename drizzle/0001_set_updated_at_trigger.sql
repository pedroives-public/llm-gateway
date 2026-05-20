CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$;

-- Trigger para a tabela tenants.
CREATE OR REPLACE TRIGGER tenants_set_updated_at
BEFORE UPDATE ON "public"."tenants"
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- Reversão comentada para referência:
-- DROP TRIGGER IF EXISTS tenants_set_updated_at ON "public"."tenants";
-- DROP FUNCTION IF EXISTS set_updated_at();