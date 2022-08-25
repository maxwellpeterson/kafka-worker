/** @type {import('ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  // Jest bug: Can't use "." in this list
  // https://stackoverflow.com/a/72437265
  moduleDirectories: ["node_modules", __dirname],
};
