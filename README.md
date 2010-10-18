partmanteau
===========
PostgreSQL Partition Management Tool

partmanteau is a set of tables and functions to simplify creation and deletion
of partitions in PostgreSQL databases. It is most definitely not a complete
solution, nor is it intended to be.

Background
----------
There are many different ways to partition a table: range, list, and hash are
three common ones. Each of these ways in turn has different tactics:

  * What is the type of the column in the range partition?
    Is it a timestamp, an integer, or some other type?
  * What hashing function is used to partition values? Which columns are arguments
    to the hashing function?
  * What columns contribute to the list items? How do the partitions correspond
    to the list items? Is it one-to-one or are some list items included in the
    same partition?
  * How often are partitions added? Timestamp-ranged partitions often add new
    partitions on a regular schedule. List partitions often have a fixed number
    of partitions corresponding to the list items and rarely changes.

More complex partitioning strategies can be implemented by combining different
tactics: partition the table by list and then partition each of list item
partitions by range.

Rather than try to solve the general problem of accomodating the large number
of partitioning tactics, partmanteau leaves the partitioning function up to the
developer.

So, what *does* partmanteau do?
-------------------------------
partmanteau provides two functions and a (mental) framework for developing a partitioning
strategy. The framework consists of clear steps which make partitioning
formulaic rather than a process reinvented each time it's needed.

partmanteau API
---------------
The two functions provided by the partmanteau API are

* `partmanteau.create_table_partition`
* `partmanteau.drop_table_partition_older_than`
* `partmanteau.insert_statement`

The `partmanteau.create_table_partition` function creates a new table,
including appropriate indexes (including key constraints), check constraints,
permissions and ownership, and table attributes such as `FILLFACTOR`. It does
*not* include foreign keys as these may (or may not) reference tables which are
themselves partitioned: determining the appropriate referenced partition
is left to the developer. It also does not include comments as the easiest
solution (copying the comment from the parent table) does not seem to add
anything to the database schema.

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

As of now, here's no general `partmanteau.drop_table_partition` function
corresponding to `partmanteau.create_table_partition`. I haven't needed to use
such a function yet as I rarely drop partitions unless they're ranged by a
timestamp column and we're aging out old partitions.

Partitioning Framework
----------------------
 1. List all table constraints as separate commands rather than as
 part of the `CREATE TABLE` statement. This makes it easier to
 determine which statements need to be included in the *create_partition*
 function.

 2. Name your indexes prefixed with the table name. partmanteau munges the index
 names when creating the indexes for the partitions and relies on the table name
 to be included in the index name.

 3. Decide on a naming strategy for partition tables. Ecapsulate this
 strategy in a function so you can easily derive a particular
 partition's name by the partitioning criteria. For example, if you're
 partitioning by a timestamp range, create a `partition_name(timestamp)`
 function returns the appropriate partition name for a value given
 timestamp. The partition name is needed in at least two places (the
 *create_partition* function and the *partition* function, so keeping
 this logic in one place (the *partition_name* function) is good
 practice.

 4. Define a *create_partition* function for the partitioned table
 that takes into account the partitioning strategy.
 This function should include:
   * the partitioning constraint (used in constraint exclusion)
   and any other table constraints.
   * foreign key constraints and any additional commands necessary
   to support the database schema.

 5. Define a *partition* function that is used to insert data into the
 appropriate table. Depending on the implementation, this function can be
 used as a trigger or as part of an insert function. If the database schema
 involves moving data between partitions, update operations may also need
 to take into account the *partition* function.

Under the covers
----------------
While the partmaneau API consists of just a handful of functions, there are
a number of other support functions and a couple of tables used to store
partitioned table metadata. These private (i.e., shouldn't be called by
user-developed functions) encapsulate the creation of statements used to
create and drop partitions as well as update the
`partmanteau.partitioned_tables` and `partmanteau.table_partitions` tables
which hold the partition metadata.

Example
=======

We have a set of monitors that each track a number of sensors which
provide temperature readings over time. This example is pulled from a
hobby project of mine to track home electricity usage using a
[Current Cost](http://www.currentcost.net/) Envi unit.

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

In keeping with the framework, the index and foreign key statements are listed
separately, and the index names are prefixed with the table name.

As the table grows, we want to drop off the oldest partitions,
keeping only the most recent data. Therefore, we'll partition the
`current_cost.readings` table by the `read_at` timestamp. We decide
to partition readings by month. We're ready to write our
*partition_name* function.

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

The `current_cost.readings_partition_name` function allows us to
derive the partition name for any given reading value.

Note that this partition name function returns names that are dependent on the
time zone of the server. If it's likely that the database will be moved to
another server in a different time zone, it would be wise to normalize this
further, perhaps determining the name based on the timestamp UTC. Such an
implemention is left as an exercise for the reader.

Now we're ready to write the *create_partition* function. This encapsulates the
logic that's particular to the partitioning strategy, providing the arguments to
the `partmanteau.create_table_partition` function.

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

As we're partitioning by timestamp, `current_cost.create_readings_partition`
takes a timestamp argument. The partition name is derived using the
`current_cost.readings_partition_name` function we already defined.

As per the framework, the partition exclusion constraint is explicitly defined
and foreign keys are included in the `partmanteau.create_table_partition`
`additional_commands` argument.

We do have a little bit of duplicated logic: when deriving the lower bound we
use `date_trunc('month', timestamp)` just like we do in the *partition_name*
function. I haven't found a good solution to eliminating this redundancy but as
the perfect is the enemy of the good, I've accepted this. Even better,
partmanteau doesn't preclude such a solution. Some clever soul will figure it out.

I also define a number of constants and variables:

 * `k_schema_name`
 * `k_base_table_name`
 * `k_partition_column`
 * `v_commands`
 * `v_partition_name`
 * `v_qualified_partition_name`
 * `k_width`, `v_lower_bound`, and `v_upper_bound`
 * `v_partition_constraint`
 * `v_additional_commands`

I find that factoring out the literals in this way makes the logic of the
function easier to see. Having implemented many partition strategies using this
framework also exposes these as the key elements in the partitioning strategy:
the logic remains largely the same. A more mature framework could store these
elements as configuration parameters in tables, further simplifying the
interface.

As an aside, I prefer the `array_to_string(expression_parts, ' ')` approach to
creating dynamic SQL statements rather than using string concatenation. It
eliminates the need to make sure I've remembered to include spaces between
elements I find there's less visual noise than having `||` peppered throughout
the statement.

I like to return the commands used to create the partition (returned by
`partmanteau.create_table_partition` as another way to see if everything is
working as expected.

We're ready to define the *partition* function. We're going to create this as a
trigger so we can issue arbitrary `INSERT` commands against the
`current_cost.readings` table. The table is also insert-only, so we don't need
to worry about `UPDATE` or `DELETE` cases.

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

The *partition* function is straightforward. After writing dynamic `INSERT`
statements enough times, I factored this out into `partmanteau.insert_statement`.
Column names are quoted as neccessary the function. To take into account the
varied column types, the values are wrapped in `quote_literal`.

Now that the partitioning strategy is set up, all that's left is to schedule the
creation and dropping of partitions. This can be as simple as setting a cron to
execute `SELECT * FROM current_cost.create_readings_partition(CURRENT_DATE + 4)`
once a month. Note the `CURRENT_DATE + 4`: to make sure the necessary partition
is in place before it's needed, we execute the command on the 28th of the month
prior to the month of the partition as `current_cost.create_readings_partition`
creates a partition based on the provided timestamp rather than on when it's
executed.

Author
------
Michael Glaesemann michael.glaesemann at myyearbook.com

Copyright and License
=====================
Copyright (c) 2010, Insider Guides, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

 * Neither the name of Insider Guides, Inc., its website properties nor the
   names of its contributors may be used to endorse or promote products derived
   from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
