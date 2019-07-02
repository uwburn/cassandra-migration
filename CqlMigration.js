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

  async execute(cassandraClients) {
    let cqlStatements = this.cql.split(/;[\r\n]+/);
    for (let cs of cqlStatements)
      await this.executeStatement(cassandraClients[0], cs);
  }

  async executeStatement(cassandraClient, cs) {
    await cassandraClient.execute(cs);
  }

};