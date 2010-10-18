BEGIN;

CREATE SCHEMA partmanteau;

CREATE FUNCTION
partmanteau.create_language_plpgsql()
RETURNS BOOLEAN
STRICT LANGUAGE sql AS $body$
/**
 *
 * Helper for CREATE LANGUAGE IF NOT EXISTS functionality for
 * systems which don't have this command.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 */
  CREATE LANGUAGE PLPGSQL;
  SELECT TRUE;
$body$;
SELECT partmanteau.create_language_plpgsql()
  WHERE NOT EXISTS (SELECT TRUE
                      FROM pg_catalog.pg_language
                      WHERE lanname = 'plpgsql');
DROP FUNCTION partmanteau.create_language_plpgsql();

CREATE TABLE partmanteau.partitioned_tables
(
  partitioned_table_id serial NOT NULL,
  schema_name text NOT NULL,
  table_name text NOT NULL
);
COMMENT ON TABLE partmanteau.partitioned_tables IS
'Lists partitioned tables which are managed through the partmanteau schema.';

CREATE UNIQUE INDEX partitioned_tables_partitioned_table_id_key
  ON partmanteau.partitioned_tables
  USING btree
  (partitioned_table_id);

CREATE UNIQUE INDEX partitioned_tables_table_name_schema_name_key
  ON partmanteau.partitioned_tables
  USING btree
  (schema_name, table_name);

CREATE TABLE partmanteau.table_partitions
(
  table_partition_id serial NOT NULL,
  partitioned_table_id integer NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  effective_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sort_key integer NOT NULL,
  table_partition_name text NOT NULL,
  CONSTRAINT table_partitions_partitioned_table_id_fkey
      FOREIGN KEY (partitioned_table_id)
      REFERENCES partmanteau.partitioned_tables (partitioned_table_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
COMMENT ON TABLE partmanteau.table_partitions IS
'Lists partitions of tables which are managed through the partmanteau schema.';
COMMENT ON COLUMN partmanteau.table_partitions.created_at IS
'When the table partition was created.';
COMMENT ON COLUMN partmanteau.table_partitions.effective_at IS
'When the table partition is expected to take effect. '
'Used primarily to determine age of a partition for tables partitioned by some time range.';
COMMENT ON COLUMN partmanteau.table_partitions.sort_key IS
'Logical ordering of the partitions.';

CREATE UNIQUE INDEX table_partitions_partitioned_table_id_table_partition_name_key
  ON partmanteau.table_partitions
  USING btree
  (partitioned_table_id, table_partition_name);

CREATE UNIQUE INDEX table_partitions_table_partition_id_key
  ON partmanteau.table_partitions
  USING btree
  (table_partition_id);

CREATE FUNCTION
partmanteau.create_table_partition(in_schema_name text,
                                   in_table_name text,
                                   in_partition_name text,
                                   in_constraints text[],
                                   in_additional_commands text[],
                                   in_effective_at timestamp with time zone,
                                   OUT command TEXT)
  RETURNS SETOF text AS
$BODY$
/**
 *
 * This function provides a wrapper for common partition creation use cases.
 * It executes the partition definition returned from
 * partmanteau.table_partition_definition and performs the requisite
 * bookkeeping on the partition metadata tables.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @param[IN]   in_schema_name     schema of the partitioned (parent) table
 * @param[IN]   in_table_name      name of the partitioned (parent) table
 * @param[IN]   in_partition_name  name for the table partition to be created
 * @param[IN]   in_constraints     Additional constraints (including the
 *                                 partitioning constraint) to be added to the
 *                                 table partition
 * @param[IN]   in_additional_commands  Additional commands to be run (such as
 *                                      trigger creation statements)
 * @param[IN]   in_effective_at    Timestamp for
 * @param[OUT]  command
 * @return
 *
 */
DECLARE
  k_create_if_necessary CONSTANT BOOLEAN := TRUE;
  v_partitioned_table_oid OID := partmanteau.table_oid(in_schema_name, in_table_name);
  v_partitioned_table_id partmanteau.partitioned_tables.partitioned_table_id%TYPE
    := partmanteau.partitioned_table_id(in_schema_name, in_table_name,
                                        k_create_if_necessary);
  v_sort_key integer;
  v_command text;
BEGIN
  FOR v_command IN
    SELECT the.cmd
      FROM partmanteau.table_partition_definition(
             v_partitioned_table_oid, in_partition_name,
             in_constraints, in_additional_commands) AS the (cmd)
  LOOP
    EXECUTE v_command;
    command := v_command || ';';
    RETURN NEXT;
  END LOOP;
  v_sort_key := COALESCE(
    (SELECT MAX(tp.sort_key)
       FROM partmanteau.table_partitions tp
       WHERE tp.partitioned_table_id = v_partitioned_table_id), 0) + 1;
  INSERT INTO partmanteau.table_partitions
    (partitioned_table_id,
     table_partition_name, effective_at, sort_key)
    VALUES (v_partitioned_table_id,
            in_partition_name, in_effective_at, v_sort_key);
  RETURN;
END
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.create_table_partition(in_schema_name text,
                                   in_table_name text,
                                   in_partition_name text,
                                   in_constraints text[],
                                   in_additional_commands text[],
                                   OUT command TEXT)
  RETURNS SETOF text AS
$BODY$
/**
 *
 * This function provides a wrapper for partmanteau.create_table_partition
 * supplying CURRENT_TIMESTAMP as in_effective_at
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @see partmanteau.create_table_partition(text, text, text, text[], text[],
 *                                         timestamp with time zone)
 *
 * @param[IN]   in_schema_name
 * @param[IN]   in_table_name
 * @param[IN]   in_partition_name
 * @param[IN]   in_constraints
 * @param[IN]   in_additional_commands
 * @param[OUT]  command
 * @return
 *
 */
  SELECT ctp.command
    FROM partmanteau.create_table_partition($1, $2, $3, $4, $5,
                                            CURRENT_TIMESTAMP) AS ctp (command)
$BODY$
  LANGUAGE sql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.delete_table_partition(in_partitioned_table_id bigint,
                                   in_table_partition_name text)
  RETURNS boolean AS
$BODY$
/**
 *
 * This function performs the required bookkeeping on the partmanteau
 * partition metadata tables when a partition is dropped. Does not actually
 * drop any tables.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_partitioned_table_id  the oid of the partitioned table
 * @param[IN]   in_table_partition_name  the name of the partition deleted.
 * @param[OUT]
 * @return      TRUE if a delete occurred on partmanteau.table_partitions
 *
 */
DECLARE
  v_partition RECORD;
  v_did_delete BOOLEAN;
BEGIN
  DELETE FROM partmanteau.table_partitions
    WHERE (partitioned_table_id, table_partition_name)
           = (in_partitioned_table_id,
              in_table_partition_name)
    RETURNING * INTO v_partition;
  v_did_delete := FOUND;
  UPDATE partmanteau.table_partitions
    SET sort_key = -1 * (sort_key - 1)
    WHERE partitioned_table_id = v_partition.partitioned_table_id
          AND sort_key > v_partition.sort_key;
  UPDATE partmanteau.table_partitions
    SET sort_key = -1 * sort_key
    WHERE partitioned_table_id = v_partition.partitioned_table_id
          AND sort_key < 0;
  RETURN v_did_delete;
END
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

CREATE FUNCTION partmanteau.table_oid(in_schema_name text, in_table_name text)
  RETURNS oid AS
$BODY$
/**
 *
 * Returns the OID for the given table. This is primarily a convenience
 * function for casting.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_schema_name
 * @param[IN]   in_table_name
 * @return
 *
 */
  SELECT CAST(CAST($1 || '.' || $2 AS regclass) AS oid)
$BODY$
  LANGUAGE sql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.drop_table_partition_older_than(in_schema_name text,
                                            in_table_name text,
                                            in_age interval)
  RETURNS boolean AS
$BODY$
/**
 *
 * This function drops the oldest partition for the given partitioned table that
 * has an effective_at age equal to or older than the given age. This is
 * primarily useful for timestamp-based range partitioned tables. It takes into
 * account effectve_at rather than the sort_key as the sort_key currently tracks
 * the order in which the partitions were created. The create_table_partition
 * code should probably be changed to update the sort_key to take into account
 * a set effective_at timestamp, but it does not currently do so.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @param[IN]   in_schema_name  the schema of the partitioned table
 * @param[IN]   in_table_name   the name of the partitioned table
 * @param[IN]   in_age          the minimum age of the partition to drop
 * @param[OUT]
 * @return      TRUE upon successful delete from the partmanteau
 *              partition metadata tables
 *
 */
DECLARE
  v_partition RECORD;
  v_sql TEXT;
BEGIN
  SELECT INTO v_partition
         tp.*
    FROM partmanteau.table_partitions tp
    WHERE tp.partitioned_table_id
            = partmanteau.partitioned_table_id(in_schema_name, in_table_name)
          AND tp.effective_at <= CURRENT_TIMESTAMP - in_age
    ORDER BY tp.effective_at
    LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'No partitions found for table %.% older than %',
                    in_schema_name, in_table_name, in_age;
  END IF;
  v_sql := 'DROP TABLE '
           || quote_ident(in_schema_name)
           || '.' || quote_ident(v_partition.table_partition_name);
  RAISE DEBUG E'DROP statement:\n%', v_sql;
  EXECUTE v_sql;
  RETURN partmanteau.delete_table_partition(v_partition.partitioned_table_id,
                                            v_partition.table_partition_name);
END
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.partitioned_table_id(IN in_schema_name text,
                                 IN in_table_name text,
                                 IN in_create_if_necessary boolean,
                                 OUT partitioned_table_id bigint)
  RETURNS bigint AS
$BODY$
/**
 *
 * This function returns the partitioned_table_id associated with the
 * given table (identified by schema_name and table_name). If a corresponding
 * row in partmanteau.partitioned_tables does not exist, one is created if
 * in_create_if_necessary is TRUE, and an exception is raise otherwise.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_schema_name
 * @param[IN]   in_table_name
 * @param[IN]   in_create_if_necessary
 * @param[OUT]  partitioned_table_id
 * @return
 *
 */
BEGIN
  SELECT INTO partitioned_table_id
         pt.partitioned_table_id
    FROM partmanteau.partitioned_tables pt
    WHERE (pt.schema_name, pt.table_name) = (in_schema_name, in_table_name);
  IF NOT FOUND THEN
    IF in_create_if_necessary THEN
      INSERT INTO partmanteau.partitioned_tables (schema_name, table_name)
        VALUES (in_schema_name, in_table_name)
        RETURNING partitioned_tables.partitioned_table_id
          INTO partitioned_table_id;
    ELSE
      RAISE EXCEPTION 'Unknown partitioned table (schema_name, table_name) = (%, %)',
            quote_literal(in_schema_name), quote_literal(in_table_name);
    END IF;
  END IF;
  RETURN;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.partitioned_table_id(in_schema_name text, in_table_name text)
  RETURNS bigint AS
$BODY$
/**
 *
 * This function returns the partitioned_table_id associated with the
 * given table (identified by schema_name and table_name). An exception
 * is raised if no corresponding partitioned table is found.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_schema_name
 * @param[IN]   in_table_name
 * @param[OUT]  partitioned_table_id
 * @return
 *
 */
  SELECT partmanteau.partitioned_table_id($1, $2, FALSE);
$BODY$
  LANGUAGE sql VOLATILE STRICT;

CREATE FUNCTION
partmanteau.table_acl_commands(IN in_rel_id oid,
                               OUT grantor text,
                               OUT acl_command text)
  RETURNS SETOF record AS
$BODY$
/**
 *
 * Returns ACL commands to apply the appropriate access to the given table.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_rel_id
 * @param[OUT]  grantor
 * @param[OUT]  acl_command
 * @return
 *
 */
SELECT grantor,
       array_to_string(CASE WHEN is_grantable
                            THEN array_append(str_parts, 'WITH GRANT OPTION')
                            ELSE str_parts END, ' ') AS acl_command
  FROM (
    SELECT grantor,
           ARRAY['GRANT', array_to_string(privilege_types, ', '),
                 'ON', table_name, 'TO', quote_ident(grantee)] AS str_parts,
           is_grantable
      FROM (SELECT u_grantor.rolname::information_schema.sql_identifier AS grantor,
                   grantee.rolname::information_schema.sql_identifier AS grantee,
                   quote_ident(nspname) || '.' || quote_ident(relname) AS table_name,
                   array_agg(privilege_type) as privilege_types,
                   aclcontains(c.relacl,
                               makeaclitem(grantee.oid,
                                           u_grantor.oid,
                                           privilege_type, true)) AS is_grantable
             FROM pg_class c
             JOIN pg_namespace nc ON c.relnamespace = nc.oid
             JOIN (SELECT pg_authid.oid, pg_authid.rolname
                     FROM pg_authid
                   UNION ALL
                   SELECT 0::oid AS oid, 'PUBLIC') grantee(oid, rolname)
               ON grantee.oid <> c.relowner
             CROSS JOIN pg_authid u_grantor
             CROSS JOIN (VALUES ('SELECT'), ('DELETE'),
                                ('INSERT'), ('UPDATE'),
                                ('REFERENCES'), ('TRIGGER')) AS pr(privilege_type)
             WHERE c.oid = $1
                   AND aclcontains(c.relacl,
                                   makeaclitem(grantee.oid,
                                               u_grantor.oid,
                                               privilege_type, false))
                   AND (pg_has_role(u_grantor.oid, 'USAGE'::text)
                        OR pg_has_role(grantee.oid, 'USAGE'::text)
                        OR grantee.rolname = 'PUBLIC'::name)
             GROUP BY u_grantor.rolname,
                      grantee.rolname,
                      table_name,
                      is_grantable) AS privs) AS cmds;
$BODY$
  LANGUAGE sql STABLE;

CREATE FUNCTION partmanteau.table_constraint_index_definitions(in_rel_id oid)
  RETURNS SETOF text AS
$BODY$
/**
 *
 * Returns the index definitions associated with table constraints
 * on the table with the given oid.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_rel_id
 *
 */
  SELECT pg_get_indexdef(indexrelid)
    FROM pg_index i
    LEFT JOIN pg_depend d
      ON i.indexrelid = d.objid
         AND d.classid = CAST('pg_catalog.pg_class' AS regclass)
         AND d.refclassid = CAST('pg_catalog.pg_constraint' AS regclass)
         AND d.deptype = 'i'
    WHERE i.indrelid = $1
         AND d.objid IS NULL;
$BODY$
  LANGUAGE sql STABLE STRICT;

CREATE FUNCTION
partmanteau.table_index_definitions(IN in_rel_id oid,
                                    IN in_exclude_constraints boolean,
                                    OUT index_name text,
                                    OUT index_definition text)
  RETURNS SETOF record AS
$BODY$
/**
 *
 * Returns index names and definitions on the table with the given oid.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_rel_id
 * @param[IN]   in_exclude_constraints  whether to exclude indexes defined
 *                                      to support UNIQUE and PRIMARY KEY constraints
 * @param[OUT]
 * @return
 *
 */
  SELECT CAST(relname AS TEXT), pg_get_indexdef(indexrelid)
    FROM pg_index i
    JOIN pg_class c ON i.indexrelid = c.oid
    LEFT JOIN pg_depend d
      ON i.indexrelid = d.objid
         AND d.classid = CAST('pg_catalog.pg_class' AS regclass)
         AND d.refclassid = CAST('pg_catalog.pg_constraint' AS regclass)
         AND d.deptype = 'i'
    WHERE i.indrelid = $1
         AND i.indisvalid
         AND CASE WHEN $2 THEN d.objid IS NULL ELSE TRUE END
$BODY$
  LANGUAGE sql STABLE STRICT;

CREATE FUNCTION
partmanteau.table_index_definitions(IN in_rel_id oid,
                                    OUT index_name text,
                                    OUT index_definition text)
  RETURNS SETOF record AS
$BODY$
/**
 *
 * Returns all index names and definitions on the table with the given oid.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_rel_id
 * @param[OUT]  index_name
 * @param[OUT]  index_definition
 * @return
 *
 */
  SELECT index_name, index_definition
    FROM partmanteau.table_index_definitions($1, FALSE)
$BODY$
  LANGUAGE sql STABLE STRICT;

CREATE FUNCTION
partmanteau.table_partition_definition(in_rel_oid oid,
                                       in_partition_name text,
                                       in_partition_constraints text[],
                                       in_additional_commands text[])
  RETURNS text AS
$BODY$
/**
 *
 * This function returns the commands necessary to create a new partition
 * for the given table. In addition to the inherited table creation statement,
 * it returns commands to
 *  - create primary key and unique constraints
 *  - create indexes
 *  - set appropriate ownership
 *  - grant required permissions
 *  - set table attributes (such as FILLFACTOR)
 *
 * These settings are based on those of the parent table.
 * It does not generate foreign key DDL or comments.
 *
 * Index creation statements assume that the given indexes are prepended
 * with the table name. This should probably be changed to generate a logical
 * index name based on the definition, but does not currently do so.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   in_rel_oid                oid of the table to partition
 * @param[IN]   in_partition_name         name of the partition to create
 * @param[IN]   in_partition_constraints  constraints added to the partition
 * @param[IN]   in_additional_commands    arbitrary commands to run after the
 *                                        partition creation
 * @param[OUT]
 * @return
 *
 */
DECLARE
  k_primary_key_contype  constant "char" := 'p';
  k_unique_contype       constant "char" := 'u';
  -- k_check_contype        constant "char" := 'c';
  -- Sometimes foreign keys are going to be to partitioned tables,
  -- and sometimes not, so we leave this up to the additional_commands argument.
  --  k_foreign_key_contype constant "char" := 'f';
  k_exclude_contraint_indexes constant BOOLEAN := TRUE;
  k_empty_text_array     constant TEXT[] := '{}';
  v_rel record;
  v_qualified_table_name text;
  v_qualified_partition_name text;
  v_cmds text[];
  v_cmd_parts text[];
  v_cmd_subparts text[];
  v_constraint record;
  v_partition_name text := in_partition_name;
  v_owner RECORD;
  v_constraint_name text;
  v_constraint_definition text;
  v_index RECORD;
  v_index_definition text;
BEGIN
  SELECT INTO v_rel
         rel.*, nspname
    FROM pg_class rel
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE rel.oid = in_rel_oid;
  IF NOT FOUND THEN
    RAISE EXCEPTION $msg$Relation with oid % not found.$msg$, in_rel_oid;
  END IF;
  RAISE DEBUG $msg$v_rel: %$msg$, v_rel;
  v_qualified_partition_name := quote_ident(v_rel.nspname)
                                  || '.' || quote_ident(v_partition_name);
  v_qualified_table_name := quote_ident(v_rel.nspname)
                              || '.' || quote_ident(v_rel.relname);
  v_cmd_parts := array_append(v_cmd_parts, 'CREATE TABLE');
  v_cmd_parts := array_append(v_cmd_parts, v_qualified_partition_name);
  v_cmd_parts := array_append(v_cmd_parts, E'(\n');

  -- add primary key and unique constraints,
  -- which aren't inherited by default in 8.3
  v_cmd_subparts := NULL;
  FOR v_constraint IN
    SELECT c.*
      FROM pg_constraint c
      WHERE c.conrelid = in_rel_oid
            AND c.contype IN (k_unique_contype,
                              k_primary_key_contype)
  LOOP
    RAISE DEBUG $msg$v_constraint: %$msg$, v_constraint;
    RAISE DEBUG $msg$v_constraint.conname: %$msg$, v_constraint.conname;
    v_constraint_name := v_constraint.conname;
    IF v_constraint.contype IN (k_unique_contype,
                                k_primary_key_contype) THEN
      -- Let Postres name primary keys and unique constraints to ensure
      -- uniqueness of the underlying index name.
      v_constraint_definition := pg_get_constraintdef(v_constraint.oid);
    ELSE
      v_constraint_definition := array_to_string(
        ARRAY['CONSTRAINT',
              quote_ident(v_constraint_name),
              pg_get_constraintdef(v_constraint.oid)], ' ');
    END IF;
    v_cmd_subparts := array_append(v_cmd_subparts, v_constraint_definition);
  END LOOP;
  -- Add partition constraint
  IF in_partition_constraints <> k_empty_text_array THEN
    FOR v_idx IN 1..array_upper(in_partition_constraints, 1) LOOP
      v_cmd_subparts := array_append(v_cmd_subparts,
                        'CHECK (' || in_partition_constraints[v_idx] || ')');
    END LOOP;
    v_cmd_parts := array_append(v_cmd_parts,
                                array_to_string(v_cmd_subparts, E',\n '));
  END IF;
  -- add FOREIGN KEY constraints
  -- FIXME not implemented
  v_cmd_parts := array_append(v_cmd_parts, E'\n)');
  v_cmd_parts := array_append(v_cmd_parts, 'INHERITS (');
  v_cmd_parts := array_append(v_cmd_parts, CAST(CAST(in_rel_oid AS regclass) AS TEXT));
  v_cmd_parts := array_append(v_cmd_parts, ')');
  IF v_rel.reloptions IS NOT NULL THEN
    v_cmd_parts := array_append(v_cmd_parts,
                   'WITH (' || array_to_string(v_rel.reloptions, ', ') || ')');
  END IF;

  v_cmds := array_append(v_cmds, array_to_string(v_cmd_parts, ' '));
  FOR v_index IN
    SELECT index_name, index_definition
      FROM partmanteau.table_index_definitions(in_rel_oid, k_exclude_contraint_indexes)
  LOOP
  -- FIXME: regexp replace doesn't catch case where index name doesn't include
  -- current table name.
  -- perhaps we should just rename the index?
    v_index_definition := replace(v_index.index_definition,
                                  'INDEX ' || v_rel.relname,
                                  'INDEX ' || in_partition_name);
    v_index_definition := replace(v_index_definition,
                                  'ON ' || quote_ident(v_rel.nspname)
                                        || '.' || quote_ident(v_rel.relname),
                                  'ON ' || v_qualified_partition_name);
    v_cmds := array_append(v_cmds, v_index_definition);
  END LOOP;
  -- set ownership
  SELECT INTO v_owner
         *
    FROM pg_authid
    WHERE oid = v_rel.relowner;
  v_cmds := array_append(v_cmds,
            array_to_string(
              ARRAY['ALTER TABLE', v_qualified_partition_name,
                    'OWNER TO', quote_ident(v_owner.rolname)], ' '));
  -- add grants
  v_cmds := array_cat(v_cmds,
                      ARRAY(SELECT replace(acl_command,
                                           'ON ' || v_qualified_table_name,
                                           'ON ' || v_qualified_partition_name)
                              FROM partmanteau.table_acl_commands(in_rel_oid)));
  -- add comment
  -- FIXME not implemented
  v_cmds := array_cat(v_cmds, in_additional_commands);
  RAISE DEBUG $msg$v_cmds: %$msg$, v_cmds;
  RETURN array_to_string(ARRAY(SELECT v_cmds[idx] || ';'
                                 FROM generate_series(1,
                                        array_upper(v_cmds, 1)) AS the (idx)), E'\n');
END
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT;

CREATE FUNCTION partmanteau.quote_ident(text[])
  RETURNS text[] AS
$BODY$
/**
 *
 * Returns a text array equivalent to the argument with
 * quote_ident applied to each element.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @private
 *
 * @param[IN]   text[]
 * @param[OUT]
 * @return
 *
 */
  SELECT ARRAY(SELECT quote_ident($1[idx])
                 FROM generate_series(1, array_upper($1, 1)) AS the (idx));
$BODY$
  LANGUAGE sql IMMUTABLE STRICT;

CREATE FUNCTION
partmanteau.insert_statement(in_schema_name text,
                             in_table_name text,
                             in_columns text[],
                             in_values text[])
  RETURNS text AS
$BODY$
/**
 *
 * Returns an INSERT statement for the given table, columns, and values.
 * The columns are processed within the function with quote_ident().
 * The values need to be processed with quote_literal *prior* to calling
 * this function if necessary.
 *
 * @author     Michael Glaesemann <michael.glaesemann@myyearbook.com>
 *
 * @param[IN]   in_schema_name
 * @param[IN]   in_table_name
 * @param[IN]   in_columns      array of text represenation of columns
 * @param[IN]   in_values       array of text represention of values
 *                              (pre-processed with quote_ident)
 * @param[OUT]
 * @return
 *
 */
  SELECT array_to_string(ARRAY['INSERT INTO',
                               quote_ident($1) || '.' || quote_ident($2),
                               '(', array_to_string(partmanteau.quote_ident($3), ','), ')',
                               'VALUES (', array_to_string($4, ','), ')'], ' ');
$BODY$
  LANGUAGE sql IMMUTABLE STRICT;

COMMIT;
