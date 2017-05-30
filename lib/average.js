exports.average = set => {
  const sum = set.reduce((sum, curr) => sum + curr, 0);
  return sum / set.length;
};
