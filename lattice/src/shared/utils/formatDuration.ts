export const formatDuration = (duration: number, isNano: boolean = false) => {
  const durationMillisec = isNano ? duration / 1000000 : duration;
  if (durationMillisec > 250) {
    return `${(durationMillisec / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} sec`;
  } else if (durationMillisec < 0) {
    return "0 ms";
  } else {
    return `${durationMillisec.toLocaleString(undefined, { maximumFractionDigits: 2 })} ms`;
  }
};
