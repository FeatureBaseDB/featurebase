export const formatTimeoutString = (timeout: string) => {
  let timeoutArr = timeout.match(/[0-9]+[dhms]/g) || [];
  const timeoutExpanded = timeoutArr.map((t) => {
    const value = Number(t.substring(0, t.length - 1));
    if (value === 0) {
      return '';
    }

    const isPlural = value > 1;
    const unit = t.substring(t.length - 1);
    switch (unit) {
      case 'd':
        return `${value} ${isPlural ? 'days' : 'day'}`;
      case 'h':
        return `${value} ${isPlural ? 'hrs' : 'hr'}`;
      case 'm':
        return `${value} ${isPlural ? 'mins' : 'min'}`;
      case 's':
        return `${value} ${isPlural ? 'secs' : 'sec'}`;
      default: return value;
    }
  });

  return timeoutExpanded.join(' ').trim() ?
    timeoutExpanded.join(' ').replace(/\s{2,}/g, ' ').trim() :
    timeout;
};
