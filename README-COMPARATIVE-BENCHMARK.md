# Panduan Dataset Komparatif Benchmark Terbaru

Panduan ini menghasilkan dataset baru yang dapat dibandingkan secara adil untuk empat kondisi system-level. Jangan mencampurkannya dengan `proposed` legacy atau hasil benchmark sebelum refactor execution-record.

| Kode | Condition CLI | Arti |
| --- | --- | --- |
| C0 | `baseline` | Aplikasi tanpa tracing. |
| C1 | `conventional` | Tracing conventional yang ada di repository. |
| C2 | `proposed-memory` | Satu execution record per boundary, buffer RAM bounded, batch asynchronous. |
| C3 | `proposed-durable` | Kontrak record C2 dengan WAL local dan asynchronous group commit. |

## Prinsip fairness

Satu dataset perbandingan hanya valid bila seluruh condition memakai workstation dan alokasi resource Docker yang sama, commit source code yang sama, workload descriptor/RPS/duration/warm-up/endpoint yang sama, jumlah repetition yang sama, serta `--measurement-mode primary`.

Jangan menjalankan condition secara manual lalu menggabungkan folder run secara acak. Matrix runner menyalakan dan mematikan stack pada setiap workload dan repetition agar state antar-run tidak terbawa.

## 1. Persiapan

1. Jalankan Docker Desktop dan tunggu hingga engine Linux siap.
2. Buka PowerShell pada root repository.
3. Pastikan tidak ada benchmark lain yang memakai port `8080`, `16686`, atau `16687`.
4. Simpan hasil lama; runner selalu membuat folder matrix baru dan tidak menimpa hasil sebelumnya.

```powershell
docker info
```

Jika perintah tersebut gagal, jangan jalankan matrix terlebih dahulu.

## 2. Primary comparison matrix

Jalankan tiga repetition untuk setiap workload dan setiap condition:

```powershell
node tools/run-matrix.mjs `
  --conditions baseline,conventional,proposed-memory,proposed-durable `
  --repetitions 3 `
  --measurement-mode primary
```

Mode `primary` memastikan `INTERNAL_OBSERVABILITY=false`. Metric diagnostic yang mahal tidak ikut membebani jalur request pada dataset utama. Runner juga mencatat validasi measurement boundary dan status saturation per run.

Untuk smoke test operasional, bukan dataset penelitian final:

```powershell
node tools/run-matrix.mjs `
  --conditions baseline,conventional,proposed-memory,proposed-durable `
  --descriptors o1-shallow-low `
  --repetitions 1 `
  --measurement-mode primary
```

## 3. Memilih data untuk analisis

Setelah matrix selesai, gunakan folder terbaru di `results/<matrix-id>/`.

| File | Kegunaan |
| --- | --- |
| `primary_results.csv` | Dataset utama latency, throughput, request sukses, CPU, dan memory. |
| `run_validation.csv` | Validitas, eligibility comparison, dan saturation per run. |
| `condition-summary.csv` | Ringkasan antar repetition. |
| `stress-test.csv` | Perilaku saturation; jangan digabung dengan steady state. |
| `failed-runs.csv` | Kegagalan teknis; bukan observasi performa. |

Untuk analisis **non-saturated performance**, gabungkan `primary_results.csv` dan `run_validation.csv` berdasarkan `run_id`, lalu pertahankan hanya:

```text
valid = true
comparison_eligible = true
saturated = false
```

Run saturated tetap penting, tetapi harus dilaporkan sebagai analisis kapasitas/stress test terpisah. Jangan menghapusnya maupun memasukkannya ke rata-rata steady state.

## 4. Diagnostic matrix terpisah

Setelah primary matrix selesai, jalankan diagnostic matrix untuk menjelaskan hasil—bukan sebagai angka utama:

```powershell
node tools/run-matrix.mjs `
  --conditions conventional,proposed-memory,proposed-durable `
  --repetitions 1 `
  --measurement-mode diagnostic
```

Gunakan `observability_results.csv`, `internal-observability-summary.csv`, dan `runs/<run-id>/observability/` untuk membaca queue pressure, worker, reconstruction, serialization, serta instrumentation. Karena diagnostic mode menambahkan pengukuran internal, hasilnya tidak boleh digabung dengan `primary_results.csv`.

## 5. Skema analisis

```text
C0 baseline ────────┐
C1 conventional ────┼── primary matrix ──> primary_results.csv
C2 proposed-memory ─┤                         + run_validation.csv
C3 proposed-durable ┘

C1/C2/C3 ───────────── diagnostic matrix ──> observability_results.csv
                                                   (penjelas, bukan pembanding utama)
```

Bandingkan C0–C3 pada request latency, business latency, achieved RPS, successful requests, CPU, dan memory. Gunakan diagnostic matrix untuk menjelaskan trade-off seperti queue pressure atau WAL. Jangan menyimpulkan suatu condition lebih baik sebelum memeriksa status valid dan saturated tiap run.

## 6. Reproduksibilitas

Simpan folder matrix lengkap, command yang dipakai, waktu eksekusi, konfigurasi Docker Desktop, dan commit hash source bersama data penelitian. Artefak V4.1 tetap eksperimen terpisah dan tidak boleh digabungkan dengan dataset ini.
