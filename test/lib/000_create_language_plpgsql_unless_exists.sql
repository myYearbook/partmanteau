CREATE FUNCTION
public.create_language_plpgsql()
RETURNS BOOLEAN
STRICT LANGUAGE sql AS $body$
  CREATE LANGUAGE PLPGSQL;
  SELECT TRUE;
$body$;
SELECT public.create_language_plpgsql()
  WHERE NOT EXISTS (SELECT TRUE 
                      FROM pg_catalog.pg_language 
                      WHERE lanname = 'plpgsql');
DROP FUNCTION public.create_language_plpgsql();
