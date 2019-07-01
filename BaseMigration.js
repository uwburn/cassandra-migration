"use strict";

const fs = require("fs");

module.exports = class BaseMigration {

  constructor(type, opts) {
    this.type = type;
    this.opts = opts;
  }

  get version() {
    return this.opts.version;
  }

  get name() {
    return this.opts.name;
  }

  static async loadFile(path) {
    return await new Promise((resolve, reject) => {
      fs.readFile(path, {
        encoding: "utf8"
      }, (err, data) => {
        if (err)
          return reject(err);

        resolve(data);
      });
    });
  }

};


