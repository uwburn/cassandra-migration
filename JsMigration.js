
"use strict";

const checksum = require("checksum");

const BaseMigration = require("./BaseMigration");

module.exports = class JsMigration extends BaseMigration {

  constructor(opts) {
    super("JS", opts);
  }

  async load() {
    this.fn = require(this.opts.path);
    this.src = await BaseMigration.loadFile(this.opts.path);
    this.checksum = checksum(this.src);
  }

  async execute(cassandraClients) {
    return this.fn(cassandraClients);
  }

};