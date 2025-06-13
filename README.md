# Final Project Big Data & Data Lakehouse

## Laporan Pengerjaan Final Project

### Anggota Kelompok:
|             Nama              |     NRP    |
|-------------------------------|------------|
| M Abhinaya Al Faruqi        | 5027231011 |
| Muhamad Rizq Taufan          | 5027231021 |
| Danar Bagus Rasendriya     | 5027231055 |
| Gandhi Ert Julio     | 5027231081 |
*** 

## Pengelolaan Data & Permasalahan Industri Game Digital: Studi Kasus Steam

### 1. Pengelolaan Data di Industri Game Digital (Steam)
Steam adalah layanan distribusi game digital terbesar di dunia dengan lebih dari 130 juta pengguna aktif bulanan dan katalog puluhan ribu game. Data yang dikelola sangat besar dan berasal dari berbagai sumber, seperti:
- **Katalog game:** judul, tanggal rilis, rating, dukungan OS, harga, diskon, dll.
- **Interaksi pengguna:** ulasan, rating, waktu bermain, wishlist, transaksi pembelian.
- **Konten komunitas:** screenshot, mod, diskusi, item workshop.
- **Data real-time:** jumlah pemain online, aktivitas event, promosi.

Pengelolaan data ini harus terintegrasi hampir real-time agar pengalaman pengguna tetap optimal.

---

### 2. Teknologi yang Digunakan dalam Proyek Kami
Untuk mensimulasikan pengelolaan data skala besar seperti Steam, kami menggunakan arsitektur berikut:
- **Apache Kafka:** Untuk ingestion dan streaming data secara real-time.
- **Apache Spark:** Untuk proses ETL, pembersihan, dan analitik data skala besar secara batch/streaming.
- **Flask:** Sebagai REST API untuk expose hasil data yang sudah diproses.
- **Streamlit:** Untuk visualisasi dashboard insight dan hasil analitik secara interaktif.

![Screenshot 2025-06-13 091348](https://github.com/user-attachments/assets/03d0a6ff-294d-40a5-8229-ea9b6f39b560)


---

### 3. Tantangan yang Dihadapi Industri
Beberapa tantangan utama di industri game digital, khususnya Steam:
- **Volume data sangat besar:** Ribuan game dan jutaan review menghasilkan data dalam skala besar.
- **Variasi data:** Data yang dikelola sangat beragam (terstruktur, semi-terstruktur, dan tidak terstruktur).
- **Kebutuhan analitik real-time:** Trend dan perubahan di marketplace harus cepat terdeteksi dan dianalisis.
- **Integrasi data:** Data berasal dari berbagai sistem dan harus bisa digabungkan serta dikelola secara efisien.
- **Kualitas & keamanan data:** Harus menjaga integritas, validitas, serta patuh pada regulasi privasi.

---

### 4. Rumusan Masalah Nyata Berdasarkan Dataset Steam
Berdasarkan dataset [Game Recommendations on Steam (Kaggle)](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam):
- **Jenis & Volume Data:**  
  Katalog game, rating, harga, diskon, review user, kompatibilitas OS, dll.  
  Volume: Puluhan ribu entri game, jutaan review, update harga & rating dinamis.
- **Masalah Nyata:**  
  - Integrasi & sinkronisasi data katalog game yang terus berubah.
  - Pengolahan dan agregasi review user dalam skala besar.
  - Analitik dan insight real-time untuk keputusan bisnis.
  - Kebutuhan filter multi-platform & multi-regional.
- **Teknologi yang Dibutuhkan:**  
  - **Kafka:** ingestion & streaming data.
  - **Spark:** pemrosesan dan analitik data skala besar.
  - **Flask:** expose data hasil ke API.
  - **Streamlit:** dashboard dan visualisasi interaktif.

---

### 5. Kesimpulan Singkat
Industri game digital seperti Steam menghadapi tantangan besar dalam integrasi dan analisis data skala masif dan format beragam. Solusi pipeline modern berbasis Kafka, Spark, Flask, dan Streamlit sangat relevan untuk mengakomodasi kebutuhan integrasi data multi-sumber, analitik real-time, serta presentasi insight secara interaktif.

---

### Referensi Singkat
- [Kaggle: Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
- Backlinko: Steam Usage Statistics (2025)
- Databricks: Data Lakehouse for Gaming Industry
- VentureBeat: How Valve Uses Big Data on Steam
