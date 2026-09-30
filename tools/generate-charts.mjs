#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { basename, resolve, join } from "node:path";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : fallback;
}

const matrixDirectory = resolve(option("--matrix-dir", ""));
if (!matrixDirectory || matrixDirectory === resolve("")) throw new Error("Usage: node tools/generate-charts.mjs --matrix-dir results/<matrix-id>");
const input = JSON.parse(await readFile(join(matrixDirectory, "analysis.json"), "utf8"));
let records = [...(input.comparisonRuns ?? input.runs ?? []), ...(input.stressTestRuns ?? [])];
// Older matrix files stored only a flattened CSV-like record and therefore lack
// scheduled request counts. Fall back to the richer matrix manifest so charts
// can still show success rate and classify historical saturation correctly.
if (records.some((record) => record.scheduled_requests === undefined)) {
  const manifest = JSON.parse(await readFile(join(matrixDirectory, "matrix-manifest.json"), "utf8"));
  const targetRps = new Map((manifest.descriptors ?? []).map((descriptor) => [descriptor.id, descriptor.rateRps]));
  records = (manifest.records ?? []).filter((record) => record.http).map((record) => {
    const http = record.http;
    const target = targetRps.get(record.descriptorId) ?? null;
    const successRate = http.scheduledRequests ? (http.successfulRequests / http.scheduledRequests) * 100 : null;
    const achievedTarget = target ? (http.achievedRps / target) * 100 : null;
    const status = (record.status === "completed" || (record.status === "failed" && http.successfulRequests > 0)) && (successRate < 95 || achievedTarget < 95) ? "saturated" : record.status;
    return {
      condition: record.condition, descriptor_id: record.descriptorId, target_rps: target, status,
      request_latency_p95_ms: http.latencyMs?.p95, success_rate_percent: successRate,
      successful_requests: http.successfulRequests, memory_bytes_mean: record.resources?.memoryBytes?.mean,
      cpu_percent_mean: record.resources?.cpuPercent?.mean, business_processing_p95_ms: http.businessProcessingMs?.p95,
      reconstruction_success_rate_percent: record.tracing?.reconstructionSuccessRatePercent
    };
  });
}
if (!records.length) throw new Error("No completed or saturated run records were found in analysis.json.");

const data = records.map((record) => ({
  condition: record.condition,
  descriptor: record.descriptor_id,
  targetRps: record.target_rps,
  status: record.status,
  latencyP95: record.request_latency_p95_ms,
  successRate: record.success_rate_percent,
  successfulRequests: record.successful_requests,
  memoryMiB: record.memory_bytes_mean === null ? null : record.memory_bytes_mean / (1024 * 1024),
  cpuPercent: record.cpu_percent_mean,
  businessP95: record.business_processing_p95_ms,
  reconstructionRate: record.reconstruction_success_rate_percent
}));

const document = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Benchmark charts - ${basename(matrixDirectory)}</title>
<style>
body{font-family:system-ui,sans-serif;margin:24px;background:#fafafa;color:#16202a}h1{font-size:22px;margin:0 0 4px}p{color:#56616b;margin:0 0 20px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(390px,1fr));gap:22px}.chart{background:#fff;border:1px solid #d9dfe5;padding:14px}.chart h2{font-size:15px;margin:0 0 6px}.chart svg{width:100%;height:auto}.legend{font-size:12px;color:#56616b;margin:4px 0 8px}.note{font-size:12px;margin-top:8px;color:#56616b}.axis{font-size:10px;fill:#56616b}.gridline{stroke:#e2e7eb;stroke-width:1}.frame{fill:none;stroke:#aeb8c2}.baseline{stroke:#2066a8}.conventional{stroke:#d46b08}.proposed{stroke:#11875d}.line{fill:none;stroke-width:2}.saturated{stroke-dasharray:6 4}.point{fill:#fff;stroke-width:2}.saturated-point{fill:#fff;stroke-width:2}.caption{font-size:11px;fill:#56616b}
</style></head><body><h1>Benchmark comparison</h1><p>Matrix: ${basename(matrixDirectory)}. Open markers identify saturated runs.</p><div class="grid" id="charts"></div>
<script>
const data=${JSON.stringify(data)};
const metrics=[
  ['latencyP95','Request latency p95','ms'],['successRate','Success rate','%'],['successfulRequests','Successful requests','requests'],['memoryMiB','Mean memory usage','MiB'],['cpuPercent','Mean CPU usage','%'],['businessP95','Business processing p95','ms'],['reconstructionRate','Reconstruction success rate','%']
];
const conditions=['baseline','conventional','proposed'];
const labels=[...new Map(data.sort((a,b)=>(a.targetRps-b.targetRps)||a.descriptor.localeCompare(b.descriptor)).map(d=>[d.descriptor+' @ '+d.targetRps+' RPS',d])).keys()];
const root=document.getElementById('charts');
const mean=a=>a.reduce((s,v)=>s+v,0)/a.length;
for(const [key,title,unit] of metrics){
  const chart=document.createElement('section');chart.className='chart';chart.innerHTML='<h2>'+title+'</h2><div class="legend">Blue: baseline | Orange: conventional | Green: proposed</div>';
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 560 300');svg.setAttribute('role','img');svg.setAttribute('aria-label',title+' by workload and condition');
  const left=62, top=16, width=470, height=194, bottom=top+height;
  const grouped=new Map();
  for(const d of data){const k=d.condition+'|'+d.descriptor+'|'+d.targetRps+'|'+d.status;const previous=grouped.get(k)||[];previous.push(d[key]);grouped.set(k,previous)}
  const points=[...grouped.entries()].map(([k,values])=>{const [condition,descriptor,targetRps,status]=k.split('|');return{condition,descriptor,targetRps:Number(targetRps),status,value:mean(values)}}).filter(d=>Number.isFinite(d.value));
  const max=Math.max(1,...points.map(d=>d.value)); const min=key==='successRate'||key==='reconstructionRate'?0:Math.min(0,...points.map(d=>d.value)); const pad=(max-min)*.08||1; const domainMax=max+pad;
  const x=i=>labels.length===1?left+width/2:left+i*width/(labels.length-1); const y=v=>bottom-(v-min)/(domainMax-min)*height;
  for(let i=0;i<=4;i++){const value=min+(domainMax-min)*i/4;const yy=y(value);svg.insertAdjacentHTML('beforeend','<line class="gridline" x1="'+left+'" x2="'+(left+width)+'" y1="'+yy+'" y2="'+yy+'"/><text class="axis" x="'+(left-8)+'" y="'+(yy+3)+'" text-anchor="end">'+value.toFixed(value<10?1:0)+'</text>')}
  svg.insertAdjacentHTML('beforeend','<rect class="frame" x="'+left+'" y="'+top+'" width="'+width+'" height="'+height+'"/><text class="caption" x="12" y="'+(top+height/2)+'" transform="rotate(-90 12 '+(top+height/2)+')">'+unit+'</text>');
  labels.forEach((label,i)=>svg.insertAdjacentHTML('beforeend','<text class="axis" x="'+x(i)+'" y="'+(bottom+18)+'" text-anchor="middle">'+label.replace(' @ ','\\n@ ').split('\\n').map((line,j)=>'<tspan x="'+x(i)+'" dy="'+(j?11:0)+'">'+line+'</tspan>').join('')+'</text>'));
  for(const condition of conditions){const values=points.filter(d=>d.condition===condition).sort((a,b)=>labels.indexOf(a.descriptor+' @ '+a.targetRps+' RPS')-labels.indexOf(b.descriptor+' @ '+b.targetRps+' RPS'));let path='';for(const point of values){const i=labels.indexOf(point.descriptor+' @ '+point.targetRps+' RPS');path+=(path?' L ':'M ')+x(i)+' '+y(point.value)}if(path)svg.insertAdjacentHTML('beforeend','<path class="line '+condition+'" d="'+path+'"/>');for(const point of values){const i=labels.indexOf(point.descriptor+' @ '+point.targetRps+' RPS');const cls=condition+(point.status==='saturated'?' saturated-point':' point');svg.insertAdjacentHTML('beforeend','<circle class="'+cls+'" cx="'+x(i)+'" cy="'+y(point.value)+'" r="4"><title>'+condition+' | '+point.descriptor+' | '+point.value.toFixed(2)+' '+unit+' | '+point.status+'</title></circle>')}}
  chart.append(svg);chart.insertAdjacentHTML('beforeend','<div class="note">Each point is the mean across repetitions. Hover a point for its value and status.</div>');root.append(chart);
}
</script></body></html>`;
const output = resolve(option("--output", join(matrixDirectory, "charts.html")));
await writeFile(output, document);
console.log(`Charts created: ${output}`);
