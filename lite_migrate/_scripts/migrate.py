#!/usr/bin/env python3

import argparse
import importlib.util
import os
import sqlite3

from lite_migrate.steps import BaseStep
from collections import defaultdict
from typing import Dict, List, Set


LM_MIGRATIONS_TABLE = "_lm_migrations"
LM_MIGRATIONS_SCHEMA = """(
    migration_name TEXT PRIMARY KEY,  -- Name of migration module / standard identifier
    migration_dt INTEGER -- UNIX timestamp of when the migration was executed
)"""
LM_INITIAL_NAME = "initial"
LM_LATEST_ALIAS = "latest"


AdjacencyDict = Dict[str, List[str]]


class ForbiddenMigrationName(Exception):
    """
    Raised if a migration with a forbidden name is found.
    """
    pass


class DependencyCycle(Exception):
    """
    Raised of a cycle is found in the migration dependency graph.
    """
    pass


def _load_migration(migrations: dict, migrations_folder: str, migration_name: str):
    m_spec = importlib.util.spec_from_file_location(
        migration_name, os.path.join(migrations_folder, f"{migration_name}.py"))
    m_mod = importlib.util.module_from_spec(m_spec)
    migrations[migration_name] = m_mod
    # noinspection PyUnresolvedReferences
    m_spec.loader.exec_module(m_mod)


def _validate_migration(migration):
    assert hasattr(migration, "up")
    assert hasattr(migration, "down")

    assert any((
        isinstance(getattr(migration, "depends_on", []), frozenset),
        isinstance(getattr(migration, "depends_on", []), list),
        isinstance(getattr(migration, "depends_on", []), set),
        isinstance(getattr(migration, "depends_on", []), tuple),
    ))

    for u in migration.up:
        assert isinstance(u, BaseStep)

    for d in migration.down:
        assert isinstance(d, BaseStep)


def _check_for_cycles_and_order(migrations: dict, aj: AdjacencyDict):
    """
    Implementation of topological-sorting DFS with an adjacency dictionary to
    put the migrations in execution order and check for cycles in the DAG.
    Adapted from https://en.wikipedia.org/wiki/Topological_sorting
    :param migrations: Migration module dictionary.
    :param aj: Adjacency dictionary
    """

    nodes_seen: Set[str] = set()
    cycle_check_set: Set[str] = set()
    node_ordering: List[str] = []

    def _visit(n: str):
        # TODO: Make this non-recursive

        if n in nodes_seen:
            return

        if n in cycle_check_set:
            # TODO: Make this more informative - node information, path
            raise DependencyCycle()

        cycle_check_set.add(n)

        for m in aj[n]:
            _visit(m)

        cycle_check_set.remove(n)
        nodes_seen.add(n)
        node_ordering.insert(0, n)

    rem_nodes = [*migrations.keys()]
    while rem_nodes:
        node = rem_nodes.pop()
        _visit(node)

    return node_ordering


def _execute_migrations(conn: sqlite3.Connection, migrations: dict, migrations_order: List[str]):
    """
    TODO
    :param conn: Connection to the database the migrations will be executed on.
    :param migrations: Migration module dictionary.
    :param migrations_order: Topologically-sorted list of migration names.
    """

    to_execute = [*migrations_order]

    while to_execute:
        for step in migrations[to_execute.pop(0)].up:
            step(cur=conn.cursor())
            conn.commit()


def main(target_migration: str):
    # TODO: Connection string
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # Open in WAL mode. This, plus using manual checkpoints, lets us commit
    # without saving any incomplete migration states to the main database file.
    # In the event of a crash, the database will be in its original state,
    # before any migration was attempted.
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA wal_autocheckpoint=0")

    # Lock the database immediately to prevent anyone else from trying to write
    # in WAL mode before we've started writing.
    cur.execute("BEGIN IMMEDIATE")

    # Create the lite_migrate migrations table if it doesn't already exist.
    cur.execute(f"CREATE TABLE IF NOT EXISTS {LM_MIGRATIONS_TABLE} {LM_MIGRATIONS_SCHEMA}")

    # Load migrations from specified module
    migrations = {}
    aj: AdjacencyDict = defaultdict(list)
    roots: list[str] = []

    # TODO: Config migrations module
    migrations_folder = "./test/migrations"

    for m in os.listdir(migrations_folder):
        m_name, m_ext = os.path.splitext(m)

        # Skip non-Python files, __init__, and the like
        if m_ext != ".py" or m_name.startswith(".") or m_name.startswith("__"):
            continue

        # Validate the migration names:
        #  - Make sure they're not called 'latest' (i.e. an alias for the most
        #    recent migration)
        if m_name == LM_LATEST_ALIAS:
            # TODO: Make this more informative
            raise ForbiddenMigrationName()

        # Load the migration module into the dictionary
        _load_migration(migrations, migrations_folder, m_name)

        # Validate migration attributes
        _validate_migration(migrations[m_name])

        # Add to relevant positions in the lookup table
        depends_on = getattr(migrations[m_name], "depends_on", [])
        if not depends_on:
            roots.append(m_name)
        for dm in depends_on:
            aj[dm].append(m_name)

    # Check for dependency cycles in the migration graph (not guaranteed to
    # be a DAG yet...) and put in topological order.
    migrations_order = _check_for_cycles_and_order(migrations, aj)

    # Execute migrations in topological order
    _execute_migrations(conn, migrations, migrations_order)

    # Write the WAL to the database and clear the file
    cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    # Restore the journal mode to the default DELETE behaviour.
    cur.execute("PRAGMA journal_mode=DELETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrates a database to the latest version or a specified "
                    "point.")
    parser.add_argument("migration_name", action="store", type=str, default=LM_LATEST_ALIAS,
                        help="The name of the migration, or 'latest'.")

    # TODO

    args = parser.parse_args()
    main(args.migration_name)
