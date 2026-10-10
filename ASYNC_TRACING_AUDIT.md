# Audit tracing asynchronous system-level

Tanggal audit: 2026-10-10. Audit ini hanya mencakup implementasi system-level.

## Perlindungan V4.1

Artefak V4.1 yang ditemukan adalah `tools/profiling-tracing-v4.mjs`. Tidak ada
referensi impor dari runtime service ke file tersebut. SHA-256 sebelum dan
sesudah perubahan ini adalah:

`F3D9352B9FDDEE8CBE29D7E51206334BD99B4A96D0961630862F81896DF14626`

File tersebut tidak diubah. `compose.proposed.yaml` dan mode `proposed` lama
juga tidak diubah agar benchmark historis tetap dapat direproduksi.

## Fakta implementasi sebelum perubahan

* C0 hanya menjalankan gateway, catalog, dan inventory tanpa tracing.
* C1 (`conventional`) adalah tracer buatan repository, **bukan OpenTelemetry**.
  Ia membuat span server/client, melakukan `JSON.stringify`, lalu `await
  fetch()` satu kali untuk setiap span ke collector. Tidak ada batching, retry,
  persistent queue, flush lifecycle, atau SDK span processor.
* C2 lama (`proposed`) membuat dua event per invocation (`ENTRY` dan `EXIT`),
  melakukan serialisasi dan memulai HTTP `fetch` per event pada jalur request.
  Queue pusat dan worker bersifat in-memory; worker melakukan polling 100 ms.
  Tidak ada local staging, acknowledgement, deduplikasi, atau recovery.
* `AsyncLocalStorage` lama membawa `requestId` dan `currentSpanId`. Wrapper
  manual berada pada handler, service, dan repository. Ia tidak dihasilkan
  oleh transformasi build-time.

## Boundary dan dampak yang terverifikasi

`x-business-processing-ms` di gateway dimulai tepat sebelum `buildQuote()` dan
berakhir setelah promise tersebut selesai. Karena wrapper lama mengerjakan UUID,
timestamp, pembuatan event, `JSON.stringify`, dan inisiasi `fetch` di dalam
`buildQuote()` dan fungsi turunannya, semua biaya itu berada pada business path
yang diukur. Queue, polling worker, dan reconstruction tidak berada pada
interval tersebut. Pada C1, ekspor span client juga berada di dalam interval
karena `ConventionalTracer.fetch()` menunggu ekspor; ekspor server dilakukan
di hook `onResponse` sehingga tidak termasuk boundary gateway tersebut.

## Perubahan system-level

`proposed-memory` (C2 baru) dan `proposed-durable` (C3) memakai kontrak
`ExecutionRecord` yang sama: satu record completion per invocation, dengan
trace ID, invocation ID, parent ID, boundary, timestamp, status, serta optional
attribute/event. Metadata minimal dan admission buffer tetap pada request path;
serialisasi batch dan network transport berada pada publisher background.

* C2 memakai bounded in-memory publisher. Buffer penuh menolak record baru;
  request bisnis tidak ditunggu, sehingga kehilangan record harus diperlakukan
  sebagai trade-off observability.
* C3 melakukan group commit append-only WAL sebelum record dipindahkan ke batch
  pengiriman. Saat startup, line `record` tanpa line `ack` dimuat kembali.
  Record belum tercatat di WAL tetap dapat hilang ketika crash; ack hanya berarti
  queue pusat menerima batch, bukan trace sudah direkonstruksi. WAL belum
  dikompaksi otomatis; retensi dan batas disk harus dipantau sebelum eksperimen
  jangka panjang.
* Queue v2 menolak kapasitas berlebih dan melakukan deduplikasi berdasarkan
  `recordId`. Reconstruction v2 menyimpan record per trace dan tidak
  menyimpulkan parent-child dari urutan kedatangan.

## Risiko dan batasan tersisa

Tidak ada transformasi TypeScript build-time pada repository ini. Boundary
masih dipilih manual melalui `withProposedSpan`; ini dipertahankan agar source
map, return value, exception, dan artefak lama tidak diubah secara luas. Sebuah
transformer harus ditambahkan sebagai eksperimen perubahan terpisah setelah
memverifikasi output AST dan coverage boundary; ia tidak boleh diam-diam
mengganti C2 lama.

Queue dan reconstruction v2 masih in-memory. C3 hanya durable di sisi producer;
queue pusat atau worker yang crash tetap dapat kehilangan record yang sudah diakui
queue. `completeTraces` saat ini berarti ada root invocation, bukan bukti bahwa
seluruh child yang mungkin telah terlambat sudah diterima. Oleh sebab itu ini
bukan klaim zero-loss maupun exactly-once end-to-end.

## Validasi yang dijalankan

* `tsc -p tsconfig.json --noEmit` berhasil.
* `docker compose ... config --quiet` untuk kedua compose baru berhasil.
* Hash V4.1 diverifikasi identik sebelum dan sesudah.

Docker stack atau benchmark tidak dijalankan oleh audit ini. Tidak ada klaim
performa atau durability berbasis benchmark runtime.
