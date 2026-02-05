// Main JavaScript for YouTube Analytics Dashboard

// Auto-hide alerts after 5 seconds
document.addEventListener('DOMContentLoaded', function() {
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(function(alert) {
        setTimeout(function() {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }, 5000);
    });
});

// Confirmation for re-analyze buttons
document.addEventListener('DOMContentLoaded', function() {
    const analyzeForms = document.querySelectorAll('form[action*="analyze"]');
    analyzeForms.forEach(function(form) {
        form.addEventListener('submit', function(e) {
            if (!confirm('Are you sure you want to run this analysis? It may take a few minutes.')) {
                e.preventDefault();
            }
        });
    });
});

// Copy video ID to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function() {
        alert('Video ID copied to clipboard: ' + text);
    });
}

// Format large numbers
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Format currency
function formatCurrency(amount) {
    return '$' + amount.toFixed(2).replace(/\d(?=(\d{3})+\.)/g, '$&,');
}
