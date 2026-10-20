# Fault-Injection Matrix: Conventional OTel vs Proposed

Dokumen ini menjalankan evaluasi ketahanan dan pemulihan, bukan benchmark performa normal. Hasilnya **tidak boleh** digabungkan ke `primary_results.csv` dari matrix sehat.

## Kondisi yang dibandingkan

| Kode | Condition | Pipeline aktual |
| --- | --- | --- |
| C1 | `conventional-otel` | OpenTelemetry SDK `BatchSpanProcessor` dan HTTP collector. |
| C2 | `proposed-memory` | execution record, buffer RAM bounded, batch asynchronous. |
| C3 | `proposed-durable` | kontrak record yang sama dengan C2, ditambah WAL local dan recovery. |

`conventional` lama tidak digunakan untuk dataset final. Ia dipertahankan hanya agar benchmark historis tetap dapat direproduksi.

## Skenario

| Nama CLI | Target pada conventional OTel | Target pada proposed | Hal yang diukur |
| --- | --- | --- | --- |
| `central-outage` | `trace-collector` dihentikan lalu dinyalakan | `trace-event-queue` dihentikan lalu dinyalakan | Ketahanan saat tujuan pengiriman pusat tidak tersedia. |
| `processing-outage` | `trace-collector` dihentikan lalu dinyalakan | `trace-reconstruction-worker` dihentikan lalu dinyalakan | Dampak penghentian komponen pemrosesan/reconstruction. Kedua pipeline tidak identik, sehingga nama service selalu disimpan di hasil. |
| `producer-restart` | `gateway` direstart | `gateway` direstart | Dampak restart producer selama ada beban aktif. |

Runner tidak mengubah queue capacity, worker count, batch size, RPS, atau descriptor. Ia hanya menyuntikkan stop/start terukur terhadap satu service.

## Menjalankan

Pastikan Docker Desktop sudah hidup dan port `8080`, `16686`, serta `16687` tidak digunakan aplikasi lain. Mulai dari satu smoke test agar mekanisme fault tervalidasi:

```powershell
pnpm fault-matrix `
  --conditions conventional-otel,proposed-memory,proposed-durable `
  --scenarios central-outage `
  --descriptor workloads/v1/o1-shallow-low.json `
  --repetitions 1
```

Setelah smoke test selesai, jalankan seluruh matriks dengan tiga pengulangan:

```powershell
pnpm fault-matrix `
  --conditions conventional-otel,proposed-memory,proposed-durable `
  --scenarios central-outage,processing-outage,producer-restart `
  --descriptor workloads/v1/o1-shallow-low.json `
  --repetitions 3
```

Default fault dimulai pada detik ke-5 dan service kembali dinyalakan setelah 5 detik. Kedua parameter dapat dibuat eksplisit, selama durasi descriptor lebih panjang daripada jumlah keduanya:

```powershell
pnpm fault-matrix --conditions conventional-otel,proposed-memory,proposed-durable --scenarios central-outage --descriptor workloads/v1/o1-shallow-low.json --repetitions 3 --fault-after-seconds 5 --outage-seconds 5
```

Runner melakukan warm-up terpisah sebelum load yang diberi fault. Setiap kombinasi memakai stack Docker baru dan dijalankan serial, lalu stack itu diturunkan sebelum kombinasi berikutnya.

## Artefak dan interpretasi

Output berada pada `results/fault-*/`:

| File | Isi |
| --- | --- |
| `fault-results.csv` | Satu baris per condition × scenario × repetition. Ini sumber tabular untuk analisis ketahanan. |
| `fault-results.json` | Salinan terstruktur dari hasil yang sama. |
| `fault-manifest.json` | Descriptor, timing fault, condition, dan scenario yang digunakan. |
| `runs/<run-id>/http-summary.json` | Respons HTTP saat fault berlangsung. |
| `runs/<run-id>/trace-diagram.svg` | Satu diagram trace hasil reconstruction untuk run tersebut bila endpoint dapat diakses. |

Kolom penting:

- `success_rate_percent` dan `successful_throughput_rps`: availability request selama gangguan.
- `recovery_time_ms`: dari stop service sampai endpoint service tersebut sehat kembali.
- `backlog_drain_time_ms`: waktu sesudah recovery hingga execution-record queue proposed kosong. Nilai OTel adalah waktu tunggu flush setelah recovery; backlog internal BatchSpanProcessor tidak diekspos sehingga tidak boleh ditafsirkan sebagai ukuran queue OTel.
- `delivery_retention_percent`: OTel = span diterima collector / estimasi 5 span untuk setiap request sukses; proposed = execution record diterima queue / seluruh record yang mencoba masuk queue. Ini dua ukuran pipeline berbeda, sehingga bandingkan sebagai **retention masing-masing mekanisme**, bukan jumlah unit mentah span versus record.
- `reconstruction_success_rate_percent`: trace lengkap / trace yang teramati oleh collector atau worker.
- `status=technical-failure`: Docker, health check, generator, atau pengumpulan artefak gagal. Baris ini bukan pengamatan performa.

Jalankan fault matrix kembali setelah memperbarui source. Runner lama membaca
nama field generator yang sudah tidak dipakai, sehingga menulis
`scheduled_requests` dan `successful_requests` sebagai kosong. Dataset dengan
dua kolom tersebut kosong tidak valid untuk mengukur availability atau retention.

Jangan menyamakan restart collector pada OTel dengan restart worker proposed sebagai failure yang identik secara arsitektur. Matriks ini menyediakan gangguan yang relevan pada peran pipeline yang bersesuaian; `target_service` disimpan agar pembaca mengetahui intervensi aktualnya.

## Memisahkan analisis

```text
healthy primary matrix  -> latency / throughput / CPU / memory pada steady state
fault-injection matrix  -> availability / retention / recovery / reconstruction saat gangguan
```

Gunakan keduanya di bab hasil sebagai dua eksperimen berbeda. Jangan mengambil rerata `fault-results.csv` lalu mencampurnya ke `primary_results.csv`.
