exports.Meter = function (message) {
  this.init = () => {
    console.log(`${message} started...`);
  };

  this.done = () => {
    const duration = (new Date()).valueOf() - this.startTime;
    console.log(`${message} done in ${duration}ms`);
  };

  this.startTime = (new Date()).valueOf();
  this.init();
};
