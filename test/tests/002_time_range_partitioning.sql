CREATE FUNCTION
partmanteau.test_timestamp_range_partition()
RETURNS SETOF TEXT
LANGUAGE PLPGSQL AS $body$
DECLARE
  k_schema_name CONSTANT TEXT := 'current_cost';
  k_partitioned_table_name CONSTANT TEXT := 'readings';
  v_sql TEXT;
  v_commands TEXT[];
  v_first_partition_timestamp TIMESTAMP WITH TIME ZONE := '2010-02-15';
  v_second_partition_timestamp TIMESTAMP WITH TIME ZONE := '2010-03-15';
  v_third_partition_timestamp TIMESTAMP WITH TIME ZONE := '2010-04-15';
  v_first_partition_table_name TEXT;
BEGIN
  v_sql := $ddl$
    CREATE SCHEMA current_cost;

    CREATE TABLE current_cost.monitors
    (
      monitor_id UUID PRIMARY KEY,
      monitor_label TEXT NOT NULL
    );

    CREATE TABLE current_cost.readings
    (
      monitor_id UUID NOT NULL,
      sensor_radio_id INT NOT NULL,
      sensor INT NOT NULL,
      reading_id UUID NOT NULL,
      read_at TIMESTAMP WITH TIME ZONE NOT NULL,
      fahrenheit REAL NOT NULL
    );

    CREATE UNIQUE INDEX readings_key ON current_cost.readings (reading_id);
    CREATE UNIQUE INDEX readings_reading_id_read_at_key
      ON current_cost.readings (reading_id, read_at);
    COMMENT ON INDEX current_cost.readings_reading_id_read_at_key IS
    'Over-constrained including read_at to allow foreign key references.';

    CREATE INDEX readings_read_at_key ON current_cost.readings (read_at);
    ALTER TABLE current_cost.readings ADD FOREIGN KEY (monitor_id)
        REFERENCES current_cost.monitors (monitor_id)
        ON DELETE CASCADE;

    CREATE FUNCTION
    current_cost.readings_partition_name(in_timestamp timestamp with time zone)
      RETURNS text AS
    $BODY$
    /**
     *
     * Returns a normalized recent readings partition name for the
     * given timestamp. Readings are partitioned by week.
     *
     * @private
     *
     * @param[IN]   in_timestamp
     * @param[OUT]
     * @return
     *
     */
      SELECT 'readings_' || to_char(date_trunc('month', $1), 'YYYYMMDD')
    $BODY$
      LANGUAGE sql IMMUTABLE STRICT;

    CREATE FUNCTION
    current_cost.create_readings_partition(in_timestamp timestamp with time zone)
      RETURNS SETOF text AS
    $BODY$
    /**
     *
     * Creates a partition for a month's worth of readings for the month
     * containing the given timestamp.
     *
     * @param[IN]   in_timestamp
     * @return
     *
     */
    DECLARE
      k_schema_name TEXT := 'current_cost';
      k_base_table_name TEXT := 'readings';
      k_partition_column TEXT := 'read_at';
      v_commands TEXT[];
      v_partition_name TEXT := current_cost.readings_partition_name(in_timestamp);
      v_qualified_partition_name TEXT := quote_ident(k_schema_name) || '.'
                                           || quote_ident(v_partition_name);
      k_width INTERVAL := '1 month';
      v_lower_bound TIMESTAMP WITH TIME ZONE := date_trunc('month', in_timestamp);
      v_upper_bound TIMESTAMP WITH TIME ZONE := v_lower_bound + k_width;
      v_partition_constraint TEXT := array_to_string(
        ARRAY[quote_ident(k_partition_column),
              '>=', quote_literal(v_lower_bound),
              'AND', quote_ident(k_partition_column),
              '<', quote_literal(v_upper_bound)], ' ');
      v_additional_commands TEXT[];
    BEGIN
      v_additional_commands := ARRAY[
        array_to_string(ARRAY['ALTER TABLE',
                              v_qualified_partition_name,
                              'ADD FOREIGN KEY (monitor_id)',
                              'REFERENCES current_cost.monitors (monitor_id)'], ' ')];
      RETURN QUERY
        SELECT command
          FROM partmanteau.create_table_partition(
                 k_schema_name, k_base_table_name,
                 v_partition_name,
                 ARRAY[v_partition_constraint],
                 v_additional_commands,
                 v_lower_bound) AS the (command);
     RETURN;
    END
    $BODY$
      LANGUAGE plpgsql VOLATILE STRICT;

    CREATE FUNCTION current_cost.partition_readings()
      RETURNS trigger AS
    $BODY$
    /**
     *
     * This trigger function partitions inserts on current_cost.readings into
     * the appropriate current_cost.readings partition.
     *
     */
    DECLARE
      k_schema CONSTANT TEXT := 'current_cost';
      k_columns CONSTANT TEXT[] := ARRAY['monitor_id',
                                         'sensor_radio_id',
                                         'sensor',
                                         'reading_id',
                                         'read_at',
                                         'fahrenheit'];
      v_values TEXT[];
      v_partition_name TEXT;
      v_sql TEXT;
    BEGIN
      IF TG_OP = 'INSERT' THEN
        v_partition_name := current_cost.readings_partition_name(NEW.read_at);
        v_values := ARRAY[quote_literal(NEW.monitor_id),
                          quote_literal(NEW.sensor_radio_id),
                          quote_literal(NEW.sensor),
                          quote_literal(NEW.reading_id),
                          quote_literal(NEW.read_at),
                          quote_literal(NEW.fahrenheit)];
        v_sql := partmanteau.insert_statement(k_schema, v_partition_name,
                                              k_columns, v_values);
        EXECUTE v_sql;
        RETURN NULL;
      END IF;
      RETURN NEW;
    END
    $BODY$
      LANGUAGE plpgsql VOLATILE STRICT;

    CREATE TRIGGER partition_readings
    BEFORE INSERT ON current_cost.readings
    FOR EACH ROW
    EXECUTE PROCEDURE current_cost.partition_readings();
$ddl$;
  RETURN NEXT lives_ok(v_sql, 'created basic schema and partitioning functions.');

  RETURN NEXT ok(NOT EXISTS (SELECT TRUE FROM partmanteau.partitioned_tables),
                 'have no partitioned tables records before creating partitions');
  RETURN NEXT ok(NOT EXISTS (SELECT TRUE FROM partmanteau.table_partitions),
                 'have no table partition records before creating partitions');
  v_commands := ARRAY(SELECT cmd
                        FROM current_cost.create_readings_partition(
                               v_first_partition_timestamp) AS the (cmd));

  v_first_partition_table_name
    := current_cost.readings_partition_name(v_first_partition_timestamp);
  RETURN NEXT is(COUNT(*), CAST(1 AS BIGINT),
                 'have a single partitioned table record after creating first partition')
    FROM partmanteau.partitioned_tables;

  RETURN NEXT is(COUNT(*), CAST(1 AS BIGINT),
                 'have a single table partition record after creating first partition')
    FROM partmanteau.table_partitions;


  RETURN NEXT ok(EXISTS (SELECT TRUE
                           FROM partmanteau.table_partitions
                           WHERE table_partition_name = v_first_partition_table_name),
    'first partition exists in table_partitions after dropping oldest partition');

  v_commands := ARRAY(SELECT cmd
                        FROM current_cost.create_readings_partition(
                               v_second_partition_timestamp) AS the (cmd));

  RETURN NEXT is(COUNT(*), CAST(1 AS BIGINT),
                 'have a single partitioned table record after creating second partition')
    FROM partmanteau.partitioned_tables;

  RETURN NEXT is(COUNT(*), CAST(2 AS BIGINT),
                 'have two table partition records after creating second partition')
    FROM partmanteau.table_partitions;

  v_commands := ARRAY(SELECT cmd
                        FROM current_cost.create_readings_partition(
                               v_third_partition_timestamp) AS the (cmd));

  RETURN NEXT is(COUNT(*), CAST(1 AS BIGINT),
                 'have a single partitioned table record after creating third partition')
    FROM partmanteau.partitioned_tables;

  RETURN NEXT is(COUNT(*), CAST(3 AS BIGINT),
                 'have three table partition records after creating third partition')
    FROM partmanteau.table_partitions;

  RETURN NEXT lives_ok('SELECT partmanteau.drop_table_partition_older_than('
               || array_to_string(ARRAY[quote_literal(k_schema_name),
                                        quote_literal(k_partitioned_table_name),
                                        quote_literal('1 month')], ',') || ')',
             'partmanteau.drop_table_partition_older_than throws no error');

  RETURN NEXT is(COUNT(*), CAST(2 AS BIGINT),
                 'have two table partition records after dropping oldest partition')
    FROM partmanteau.table_partitions;

  RETURN NEXT ok(NOT EXISTS (SELECT TRUE
                               FROM partmanteau.table_partitions
                               WHERE table_partition_name = v_first_partition_table_name),
    'first partition does not exist in table_partitions after dropping oldest partition');

  RETURN;
END
$body$;


