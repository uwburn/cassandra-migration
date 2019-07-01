"use strict";

const checksum = require("checksum");

const BaseMigration = require("./BaseMigration");

module.exports = class CqlMigration extends BaseMigration {

  constructor(opts) {
    super("CQL", opts);
  }

  async load() {
    this.cql = await BaseMigration.loadFile(this.opts.path);
    this.checksum = checksum(this.cql);
  }

  async execute(cassandraClient) {
    let cqlStatements = this.cql.split(";\n");
    for (let cs of cqlStatements)
      await this.executeStatement(cassandraClient, cs);
  }

  async executeStatement(cassandraClient, cs) {
    await cassandraClient.execute(cs);
  }

};