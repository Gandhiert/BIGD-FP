# 🎮 Steam Games Recommendation Frontend

Frontend UI yang modern dan responsif untuk sistem rekomendasi game Steam menggunakan machine learning clustering.

## 📁 Struktur File

```
frontend/
├── index.html      # Main HTML file
├── styles.css      # CSS styling
├── script.js       # JavaScript functionality
└── README.md       # Documentation
```

## ✨ Fitur

- **🔍 Smart Search**: Autocomplete dengan suggestion dropdown
- **📊 Game Info**: Informasi detail game yang dicari
- **🎯 ML Recommendations**: Rekomendasi berbasis clustering machine learning
- **📱 Responsive Design**: Mendukung desktop, tablet, dan mobile
- **⚡ Real-time**: Data real-time dari API backend
- **🎨 Modern UI**: Design yang cantik dengan gradient dan animasi

## 🚀 Cara Menggunakan

### 1. Setup Backend API
Pastikan API backend sudah berjalan di `http://localhost:5000`:

```bash
# Di terminal pertama - jalankan clustering system (untuk generate clusters data)
./run_games_clustering_system.sh start

# Di terminal kedua - jalankan API server
cd data_lake/scripts
python analytics_api.py
```

**Note**: API sekarang berjalan di port 5000 dengan endpoint baru yang lebih user-friendly!

### 2. Buka Frontend
Buka file `index.html` di browser:

- **Option 1**: Double-click `index.html`
- **Option 2**: Gunakan live server (recommended)
- **Option 3**: Serve dengan Python:
  ```bash
  cd frontend
  python -m http.server 8000
  # Buka http://localhost:8000
  ```

### 3. Gunakan Interface

1. **Ketik nama game** di search box
2. **Pilih dari suggestions** yang muncul
3. **Klik search** atau tekan Enter
4. **Lihat rekomendasi** yang muncul berdasarkan clustering ML

## 🎯 Contoh Penggunaan

### Game Populer untuk Dicoba:
- `Counter-Strike`
- `Dota 2`
- `PUBG`
- `Among Us`
- `Fall Guys`
- `Cyberpunk 2077`
- `The Witcher 3`

## 📱 Responsive Design

UI ini dirancang untuk bekerja optimal di:
- **Desktop**: Full feature dengan grid layout
- **Tablet**: Adaptasi layout yang nyaman
- **Mobile**: Single column layout yang mudah digunakan

## ⌨️ Keyboard Shortcuts

- **Ctrl/Cmd + K**: Focus ke search input
- **Escape**: Clear search dan hide suggestions
- **Enter**: Execute search

## 🔧 Konfigurasi

### API Base URL
Ubah di `script.js` jika API berjalan di port/host berbeda:

```javascript
const API_BASE_URL = 'http://localhost:5000';
```

### API Endpoints yang Digunakan
- `GET /api/clustering/games` - Mendapatkan daftar games untuk autocomplete
- `GET /api/recommendations/<game_title>` - Mendapatkan rekomendasi berdasarkan nama game
- `GET /api/games/search?q=<query>` - Mencari games berdasarkan nama (fallback)

### Styling
Customization CSS di `styles.css`:
- Color scheme: Edit gradient colors
- Layout: Modify grid templates
- Animations: Adjust transition timings

## 🚨 Troubleshooting

### Error: "Pastikan API server berjalan"
- Cek apakah clustering system dan API sudah running
- Verifikasi port 5000 tidak digunakan aplikasi lain
- Test API endpoint: `curl http://localhost:5000/api/clustering/games?limit=5`
- Test recommendation: `curl "http://localhost:5000/api/recommendations/Counter-Strike"`

### Autocomplete tidak muncul
- Tunggu API response untuk load games list
- Ketik minimal 2 karakter
- Check browser console untuk error

### CORS Issues
Jika ada CORS error, gunakan live server atau Python server instead of file://

## 📊 Data Flow

```
User Input → Autocomplete → API Request → ML Processing → Results Display
     ↓              ↓            ↓              ↓              ↓
  Search Box → Suggestions → /recommendations → Clustering → Game Cards
```

## 🎨 UI Components

- **Header**: Gradient background dengan branding
- **Search Section**: Input field dengan autocomplete
- **Game Info Card**: Detail game yang dicari
- **Recommendations Grid**: Card-based layout untuk hasil
- **Loading States**: Spinner dan feedback visual
- **Error Handling**: User-friendly error messages

## 🔮 Future Enhancements

- [ ] Dark mode toggle
- [ ] Filter by price/rating
- [ ] Save favorites
- [ ] Share recommendations
- [ ] Game comparison feature
- [ ] Advanced search filters

---

**Made with ❤️ for Steam Gaming Analytics** 