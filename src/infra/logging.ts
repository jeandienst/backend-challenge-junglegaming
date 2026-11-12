export type LogLevel='info'|'error'|'warn';
export function logEvent(level:LogLevel,fields:Record<string,unknown>){
  const line=JSON.stringify({timestamp:new Date().toISOString(),level,...fields});
  if(level==='error')console.error(line);else console.log(line);
}
