const Meter = require('cli-meter');
const process = require('process');

function MeterHelper(message, total) {
  this.m = new Meter({ total });
  this.total = total;
  process.stdout.moveCursor(0, 1);

  this.step = (n) => {
    this.m.step(n);
    process.stdout.write(`${message}\t${this.m}\n`);
    process.stdout.moveCursor(0, -1);
  };

  this.set = (n) => {
    this.m.set(n);
  };

  this.done = () => {
    process.stdout.moveCursor(0, 1);
  };
}

module.exports = {
  Meter: MeterHelper
};
