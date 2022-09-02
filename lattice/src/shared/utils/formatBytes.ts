export const formatBytes = (bytes: number, decimals: number = 2) => {
  if (bytes <= 0) {
    return '0 Bytes';
  }

  const kb = 1024;
  const decimalPlaces = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  let sizeIndex = Math.floor(Math.log(bytes) / Math.log(kb));

  if(sizeIndex >= sizes.length-1) {
    sizeIndex = sizes.length-1;
  }

  return parseFloat((bytes / Math.pow(kb, sizeIndex)).toFixed(decimalPlaces)) + ' ' + sizes[sizeIndex];
}
