"use strict";

const debug = require("debug")("cassandra-migration");

const fs = require("fs");
const path = require("path");

const JsMigration = require("./JsMigration");
const CqlMigration = require("./CqlMigration");

async function ensureKeyspace(cassandraClient, keyspace) {
  debug(`Ensuring keyspace ${keyspace}, if not exists it will be created with default settings`);

  await cassandraClient.execute(`CREATE KEYSPACE IF NOT EXISTS ${keyspace}
  WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
  }`);
}

async function useKeyspace(cassandraClient, keyspace) {
  debug(`Using keyspace ${keyspace}`);

  await cassandraClient.execute(`USE ${keyspace}`);
}

async function ensureMigrationTable(cassandraClient) {
  debug("Ensuring table migration_history exists");

  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS migration_history (
    version int,
    name text,
    type text,
    checksum text,
    installed_on timestamp,
    execution_time int,
    success boolean,
    PRIMARY KEY (version)
  )`);
}

async function getAppliedMigrations(cassandraClient) {
  debug("Retrieving table applied migration list");

  return (await cassandraClient.execute("SELECT * FROM migration_history")).rows;
}

async function loadMigration(dir, filename) {
  let match = filename.match(/^([0-9]+)__([A-z0-9_]*)\.(js|cql)$/);
  if (match.index !== 0)
    return;

  let type = match[3];
    
  let opts = {
    path: dir + path.sep + filename,
    version: parseInt(match[1]),
    name: match[2]
  };

  debug(`Loading migration definition ${filename}`);

  let migration;
  switch(type) {
  case "js":
    migration = new JsMigration(opts);
    break;
  case "cql":
    migration = new CqlMigration(opts);
    break;
  }

  await migration.load();

  return migration;
}

async function loadMigrations(dir) {
  debug("Loading migration definitions");

  let migrationFiles = fs.readdirSync(dir);

  let migrations = [];
  for (let file of migrationFiles) {
    let m = await loadMigration(dir, file);
    if (m)
      migrations.push(m);
  }

  return migrations;
}

function checkAppliedMigrations(appliedMigrations, migrations) {
  debug("Checking applied migrations");

  if (appliedMigrations.length === 0)
    return;

  let lm = appliedMigrations[appliedMigrations.length - 1];
  if (!lm.success)
    throw new Error(`Migration ${lm.version} - "${lm.name}" failed, fix manually before retrying`);

  for (let i = 0; i < appliedMigrations.length; ++i) {
    let am = appliedMigrations[i];
    let m = migrations[i];

    if (am.version !== m.version)
      throw new Error(`Migration version mismatch: applied ${am.version}, defined ${m.version}`);

    if (am.name !== m.name)
      throw new Error(`Migration name mismatch: applied ${am.name}, defined ${m.name}`);

    if (am.checksum !== m.checksum)
      throw new Error(`Migration checksum mismatch: applied ${am.checksum}, defined ${m.checksum}`);
  }
}

async function executeMigration(cassandraClient, migration) {
  debug(`Applying migration ${migration.version} "${migration.name}"`);

  let now = new Date();

  let cachedError;
  let success;
  try {
    await migration.execute(cassandraClient);
    success = true;
  }
  catch(err) {
    cachedError = err;
    success = false;
  }

  await cassandraClient.execute(`INSERT INTO migration_history (version, name, type, checksum, installed_on, execution_time, success) VALUES (
    ?, ?, ?, ?, ?, ?, ?
  )`, [
    migration.version,
    migration.name,
    migration.type,
    migration.checksum,
    now,
    new Date() - now,
    success
  ], {
    prepare: true
  });

  if (!success)
    throw cachedError;
}

module.exports = async function(cassandraClient, opts) {
  if (opts.ensureKeyspace)
    await ensureKeyspace(cassandraClient, opts.keyspace);

  if (opts.useKeyspace)
    await useKeyspace(cassandraClient, opts.keyspace);

  await ensureMigrationTable(cassandraClient);

  let appliedMigrations = await getAppliedMigrations(cassandraClient);
  let migrations = await loadMigrations(opts.dir);

  checkAppliedMigrations(appliedMigrations, migrations);

  for (let i = appliedMigrations.length; i < migrations.length; ++i) {
    let m = migrations[i];
    await executeMigration(cassandraClient, m);
  }

  debug("Migrations applied succeffully");
};