CREATE FUNCTION
partmanteau.test_truth()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
BEGIN
  RETURN NEXT ok(TRUE, 'testing truth');
  RETURN;
END
$body$;
