// Configuration
const API_BASE_URL = 'http://localhost:5000';
let allGames = [];
let searchTimeout;

// Load semua games untuk autocomplete
async function loadAllGames() {
    try {
        // Use the new clustering games endpoint to get games list
        const response = await fetch(`${API_BASE_URL}/api/clustering/games?limit=1000`);
        if (response.ok) {
            const data = await response.json();
            allGames = data.games || [];
            console.log(`Loaded ${allGames.length} games for autocomplete`);
        }
    } catch (error) {
        console.log('Could not load games list:', error);
        // Fallback: try to get games from search endpoint
        try {
            const fallbackResponse = await fetch(`${API_BASE_URL}/api/games/search?q=a&limit=100`);
            if (fallbackResponse.ok) {
                const fallbackData = await fallbackResponse.json();
                allGames = fallbackData.games || [];
                console.log(`Loaded ${allGames.length} games via fallback search`);
            }
        } catch (fallbackError) {
            console.log('Fallback games loading also failed:', fallbackError);
        }
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadAllGames();
    
    const searchInput = document.getElementById('gameSearch');
    searchInput.addEventListener('input', handleSearchInput);
    searchInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            searchGame();
        }
    });

    // Hide suggestions when clicking outside
    document.addEventListener('click', function(e) {
        if (!e.target.closest('.search-container')) {
            hideSuggestions();
        }
    });
});

function handleSearchInput(e) {
    const query = e.target.value.trim();
    
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
        if (query.length >= 2) {
            showSuggestions(query);
        } else {
            hideSuggestions();
        }
    }, 300);
}

function showSuggestions(query) {
    const suggestions = allGames
        .filter(game => 
            game.title.toLowerCase().includes(query.toLowerCase())
        )
        .slice(0, 8);

    const suggestionsDiv = document.getElementById('suggestions');
    
    if (suggestions.length > 0) {
        suggestionsDiv.innerHTML = suggestions
            .map(game => `
                <div class="suggestion-item" onclick="selectGame('${game.title.replace(/'/g, "\\'")}')">
                    ${escapeHtml(game.title)}
                </div>
            `).join('');
        suggestionsDiv.style.display = 'block';
    } else {
        hideSuggestions();
    }
}

function selectGame(gameTitle) {
    document.getElementById('gameSearch').value = gameTitle;
    hideSuggestions();
    searchGame();
}

function hideSuggestions() {
    document.getElementById('suggestions').style.display = 'none';
}

async function searchGame() {
    const gameTitle = document.getElementById('gameSearch').value.trim();
    
    if (!gameTitle) {
        showError('Silakan masukkan nama game terlebih dahulu.');
        return;
    }

    hideAllSections();
    showLoading();

    try {
        // Use the new recommendations endpoint that accepts game title
        const response = await fetch(`${API_BASE_URL}/api/recommendations/${encodeURIComponent(gameTitle)}?limit=8`);
        const data = await response.json();

        hideLoading();

        if (response.ok && data.recommendations && data.recommendations.length > 0) {
            displayResults(data);
        } else if (response.status === 404) {
            // Try to show suggestions if game not found
            if (data.suggestions) {
                showError(`Game "${gameTitle}" tidak ditemukan. ${data.suggestions}`);
            } else {
                showNoResults();
            }
        } else {
            showError(data.error || 'Tidak dapat mengambil rekomendasi untuk game tersebut.');
        }
    } catch (error) {
        hideLoading();
        showError('Terjadi kesalahan saat mengambil data. Pastikan API server berjalan di ' + API_BASE_URL);
        console.error('API Error:', error);
    }
}

function displayResults(data) {
    const gameInfo = data.game_info;
    const recommendations = data.recommendations;
    const searchInfo = data.search_info;

    // Display game info with search match information
    const matchInfo = searchInfo ? 
        `<small style="color: #666; display: block; margin-top: 5px;">
            <i class="fas fa-search"></i> Pencarian: "${searchInfo.searched_title}" → Ditemukan: "${searchInfo.matched_title}"
            ${searchInfo.total_matches > 1 ? ` (${searchInfo.total_matches} game serupa ditemukan)` : ''}
        </small>` : '';

    document.getElementById('gameInfo').innerHTML = `
        <h3><i class="fas fa-info-circle"></i> Informasi Game: ${escapeHtml(gameInfo.title)}</h3>
        ${matchInfo}
        <div class="game-details">
            <div class="detail-item">
                <div class="detail-label">Rating</div>
                <div class="detail-value">${gameInfo.rating || 'N/A'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Harga</div>
                <div class="detail-value">$${(gameInfo.price_final || 0).toFixed(2)}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Positive Ratio</div>
                <div class="detail-value">${(gameInfo.positive_ratio || 0).toFixed(1)}%</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">User Reviews</div>
                <div class="detail-value">${(gameInfo.user_reviews || 0).toLocaleString()}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Cluster ID</div>
                <div class="detail-value">${gameInfo.cluster}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">App ID</div>
                <div class="detail-value">${gameInfo.app_id}</div>
            </div>
        </div>
    `;

    // Display recommendations
    document.getElementById('recommendations').innerHTML = recommendations
        .map(game => `
            <div class="game-card">
                <div class="game-card-header">
                    <div class="game-title">${escapeHtml(game.title)}</div>
                    <div class="similarity-score">
                        <i class="fas fa-chart-line"></i> 
                        Similarity: ${(game.similarity_score * 100).toFixed(1)}%
                    </div>
                </div>
                <div class="game-card-body">
                    <div class="game-stats">
                        <div class="stat-item">
                            <div class="stat-label">Rating</div>
                            <div class="stat-value">${game.rating || 'N/A'}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Reviews</div>
                            <div class="stat-value">${(game.user_reviews || 0).toLocaleString()}</div>
                        </div>
                    </div>
                    <div class="game-stats">
                        <div class="stat-item">
                            <div class="stat-label">Positive Ratio</div>
                            <div class="stat-value">${(game.positive_ratio || 0).toFixed(1)}%</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Cluster</div>
                            <div class="stat-value">${game.cluster}</div>
                        </div>
                    </div>
                    <div class="price-tag ${game.price_final === 0 ? 'price-free' : ''}">
                        ${game.price_final === 0 ? 'Free to Play' : '$' + game.price_final.toFixed(2)}
                    </div>
                </div>
            </div>
        `).join('');

    document.getElementById('results').style.display = 'block';
    
    // Log search info for debugging
    if (searchInfo) {
        console.log('Search completed:', {
            searched: searchInfo.searched_title,
            matched: searchInfo.matched_title,
            match_score: searchInfo.match_score,
            total_matches: searchInfo.total_matches
        });
    }
}

function showLoading() {
    document.getElementById('loading').style.display = 'block';
}

function hideLoading() {
    document.getElementById('loading').style.display = 'none';
}

function showError(message) {
    const errorDiv = document.getElementById('error');
    errorDiv.innerHTML = `<i class="fas fa-exclamation-triangle"></i> ${message}`;
    errorDiv.style.display = 'block';
}

function showNoResults() {
    document.getElementById('noResults').style.display = 'block';
}

function hideAllSections() {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'none';
    document.getElementById('results').style.display = 'none';
    document.getElementById('noResults').style.display = 'none';
    hideSuggestions();
}

function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    // Focus search input when pressing Ctrl/Cmd + K
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        document.getElementById('gameSearch').focus();
    }
    
    // Clear search when pressing Escape
    if (e.key === 'Escape') {
        document.getElementById('gameSearch').value = '';
        hideAllSections();
        hideSuggestions();
    }
});

// Add some debug info
console.log('🎮 Steam Games Recommendation UI loaded');
console.log('API Base URL:', API_BASE_URL); 