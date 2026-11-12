import { Injectable } from '@nestjs/common';

@Injectable()
export class Metrics {
  private readonly counters=new Map<string,bigint>();
  private readonly gauges=new Map<string,number>();
  private readonly hist=new Map<string,{count:number;sum:number}>();
  inc(name:string,labels:Record<string,string>={}){const key=this.key(name,labels);this.counters.set(key,(this.counters.get(key)??0n)+1n);}
  gauge(name:string,value:number,labels:Record<string,string>={}){this.gauges.set(this.key(name,labels),value);}
  observe(name:string,value:number,labels:Record<string,string>={}){const key=this.key(name,labels),h=this.hist.get(key)??{count:0,sum:0};h.count++;h.sum+=value;this.hist.set(key,h);}
  private key(n:string,l:Record<string,string>){return `${n}${Object.keys(l).length?`{${Object.entries(l).map(([k,v])=>`${k}="${v}"`).join(',')}}`:''}`;}
  render(){const rows:string[]=[];for(const [k,v] of this.counters)rows.push(`${k} ${v}`);for(const [k,v] of this.gauges)rows.push(`${k} ${v}`);for(const [k,v] of this.hist){rows.push(`${k}_count ${v.count}`,`${k}_sum ${v.sum}`);}return rows.join('\n')+'\n';}
}
