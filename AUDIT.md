# Audit Benchmark Microservices

Tanggal audit: 2026-09-07

## Ringkasan

Repository berisi aplikasi benchmark tiga service untuk baseline, conventional tracing, dan proposed tracing. File repository ini adalah source of truth. Direktori ini bukan Git repository, sehingga revision identifier belum dapat direkam di manifest.

Pilot belum dijalankan: Docker client tersedia, tetapi Docker daemon tidak dapat diakses dari sesi audit ini. Tidak ada angka benchmark yang diklaim.

## Kesesuaian terhadap proposal

| Aspek | Proposal | Implementasi | Status | Catatan |
| --- | --- | --- | --- | --- |
| Boundary instrumentation | Handler, service, repository | `withProposedSpan` membungkus ketiga layer | Sesuai | Workload structural nodes ada di layer service. |
| Event model | Entry dan exit | `ENTRY`/`EXIT` plus ID, parent, nama, waktu, service | Sesuai | Worker memasangkan event per `span_id`. |
| Context propagation | Request context dan parent-child | `AsyncLocalStorage` dan header antar-service | Sesuai | Gateway meneruskan active span. |
| Async processing | Queue dan worker terpisah | Submission tidak ditunggu, queue bounded, worker terpisah | Sesuai | HTTP response tidak menunggu reconstruction. |
| Call graph | Relasi parent-child | Nodes/edges dibentuk per request | Sesuai | Trace lengkap perlu semua pasangan dan parent. |
| Conventional | Service-to-service tracing | Server/client span diekspor dengan `await` | Sesuai | Tanpa wrapper internal atau worker. |
| Baseline | Tanpa tracing | `compose.yaml` tanpa queue/worker/collector | Sesuai | Kontrol bersih. |
| Lingkungan final | Ubuntu Server 22.04 | Audit memakai Windows + Docker Desktop | Deviasi | Hasil final harus melaporkan lingkungan aktual. |

## Experimental Design Decisions

- Node.js 22, TypeScript, Fastify, Docker Compose, dan pnpm.
- Topologi Gateway, Catalog, dan Inventory serta repository in-memory deterministik untuk mengurangi confounding factor database eksternal.
- Baseline sebagai kondisi kontrol terpisah.
- Descriptor workload versioned, input, call shape, dan intensity.

## Audit workload

- `O(1)`, `O(log n)`, `O(n)`, dan `mixed` memakai loop deterministik.
- `shallow`, `moderate`, dan `complex` mengubah `boundaryDepth`/`fanOut`, dan executor benar-benar menjalankan node-node tersebut.
- `mixed-complex-high` memakai `mixed-complex-v1`: logaritmik lalu linear cost, structure complex, fan-out 3.
- Tiga descriptor `on-*-n512-low` mengisolasi structural signature dengan compute, input, dan intensity yang sama.

## Perbaikan audit ini

Sebelumnya runner memasukkan successful warm-up requests ke denominator Reconstruction Success Rate dan menyimpan counter tracing kumulatif. Ini mencampurkan warm-up dengan pengukuran.

Runner kini melakukan warm-up, menunggu queue drain, mengambil snapshot, lalu menjalankan pengukuran tanpa warm-up. `tracing-metrics.json` menyimpan delta measurement-only dan membedakan event produced, enqueued, dropped, dan reconstructed. `http-summary.json` menyimpan Request Latency (request start sampai HTTP response) serta Business Processing Time (Gateway `buildQuote` start sampai quote siap). Keduanya tidak mencakup waktu hingga worker selesai.

## Automated matrix dan pilot setelah Docker tersedia

Jalankan `pnpm matrix` untuk mengeksekusi baseline, conventional, dan proposed secara berurutan pada seluruh descriptor, termasuk mixed workload serta variasi low/mid/high RPS. Descriptor dijalankan dari low ke high RPS dan runner memulai/menghentikan stack untuk setiap workload repetition agar state overload tidak terbawa. Raw artifact tiap run disimpan; hanya pengukuran valid masuk `analysis.json` dan `analysis.csv`, sedangkan kegagalan dicatat pada `failed-runs.csv`. Untuk pilot kecil gunakan `pnpm matrix -- --descriptors o1-shallow-low --cooldown-seconds 0`. Pilot memvalidasi pipeline, bukan menghasilkan hasil final.

Untuk studi final, dokumentasikan warm-up, durasi, pengulangan, cooldown, urutan/randomisasi, reset state, seed, dan lingkungan aktual. Matrix penuh adalah `condition x computational signature x structural signature x input size x request intensity`; alasan penggunaan subset harus dicatat bersama descriptor hash.
