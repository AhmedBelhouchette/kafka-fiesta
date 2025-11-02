/**
 * Resource Manager Interface - Frontend JavaScript
 * Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
 * Date: 2025-11-02
 */

const API_BASE = 'http://localhost:8000/api';
let autoRefreshInterval = null;

// ==================== MISE À JOUR DE L'HEURE ====================

function updateTime() {
    const now = new Date();
    const timeString = now.toLocaleString('fr-FR', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
    
    const dateElement = document.getElementById('current-date');
    if (dateElement) {
        dateElement.textContent = timeString;
    }
}

// ==================== STATISTIQUES ====================

async function fetchStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        const data = await response.json();
        
        // Machines
        if (data.machines) {
            updateStatElement('stat-disponible', data.machines.disponible || 0);
            updateStatElement('stat-assignee', data.machines.assignee || 0);
            updateStatElement('stat-pause', data.machines.pause || 0);
            updateStatElement('stat-arret', data.machines.arret || 0);
        }
        
        // Tâches
        if (data.tasks) {
            updateStatElement('stat-completed', data.tasks.completed || 0);
            updateStatElement('stat-interrupted', data.tasks.interrupted || 0);
            updateStatElement('queue-size', data.tasks.in_queue || 0);
        }
        
    } catch (error) {
        console.error('Erreur récupération stats:', error);
    }
}

function updateStatElement(id, value) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = value;
    }
}

// ==================== MACHINES ====================

async function fetchMachines() {
    try {
        const response = await fetch(`${API_BASE}/machines`);
        const data = await response.json();
        
        const container = document.getElementById('machines-grid');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (data.machines && data.machines.length > 0) {
            data.machines.forEach(machine => {
                const card = createMachineCard(machine);
                container.appendChild(card);
            });
        }
        
    } catch (error) {
        console.error('Erreur récupération machines:', error);
    }
}

function createMachineCard(machine) {
    const card = document.createElement('div');
    card.className = `machine-card ${machine.status.toLowerCase()}`;
    
    const statusEmoji = {
        'DISPONIBLE': '🟢',
        'ASSIGNEE': '🔵',
        'PAUSE': '🟡',
        'ARRET': '🔴',
        'IDLE': '⚪'
    };
    
    const emoji = statusEmoji[machine.status] || '⚪';
    
    let cardContent = `
        <div class="machine-header">
            <h3>${machine.id}</h3>
            <span class="status-badge ${machine.status.toLowerCase()}">${emoji} ${machine.status}</span>
        </div>
        <div class="machine-metrics">
            <div class="metric">
                <span class="metric-label">🌡️ Température:</span>
                <span class="metric-value">${machine.temperature}°C</span>
            </div>
            <div class="metric">
                <span class="metric-label">⚡ Charge:</span>
                <span class="metric-value">${machine.charge}%</span>
            </div>
    `;
    
    if (machine.task) {
        cardContent += `
            <div class="metric">
                <span class="metric-label">📦 Tâche:</span>
                <span class="metric-value">${machine.task}</span>
            </div>
        `;
    }
    
    if (machine.time_remaining !== undefined) {
        const minutes = Math.floor(machine.time_remaining / 60);
        const seconds = machine.time_remaining % 60;
        cardContent += `
            <div class="metric">
                <span class="metric-label">⏱️ Temps restant:</span>
                <span class="metric-value">${minutes}m ${seconds}s</span>
            </div>
        `;
    }
    
    cardContent += `</div>`;
    
    if (machine.status === 'ARRET') {
        cardContent += `
            <div class="machine-actions">
                <button class="btn-repair" onclick="repairMachine('${machine.id}')">
                    🔧 RÉPARER
                </button>
            </div>
        `;
    }
    
    card.innerHTML = cardContent;
    return card;
}

async function repairMachine(machineId) {
    if (!confirm(`Êtes-vous sûr de vouloir réparer ${machineId} ?`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/machines/${machineId}/repair`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            alert('✅ ' + data.message);
            fetchMachines();
            fetchStats();
        }
    } catch (error) {
        alert('❌ Erreur lors de la réparation: ' + error);
    }
}

// ==================== TÂCHES ====================

async function fetchTasks() {
    try {
        const response = await fetch(`${API_BASE}/tasks`);
        const data = await response.json();
        
        // File d'attente
        const queueContainer = document.getElementById('task-queue');
        if (queueContainer) {
            queueContainer.innerHTML = '';
            
            if (data.queue && data.queue.length > 0) {
                data.queue.forEach(task => {
                    const card = createTaskCard(task, 'queue');
                    queueContainer.appendChild(card);
                });
            } else {
                queueContainer.innerHTML = '<p class="empty-message">Aucune tâche en attente</p>';
            }
        }
        
        // Tâches en cours
        const inProgressContainer = document.getElementById('tasks-in-progress');
        if (inProgressContainer) {
            inProgressContainer.innerHTML = '';
            
            if (data.in_progress && data.in_progress.length > 0) {
                data.in_progress.forEach(task => {
                    const card = createTaskCard(task, 'progress');
                    inProgressContainer.appendChild(card);
                });
            } else {
                inProgressContainer.innerHTML = '<p class="empty-message">Aucune tâche en cours</p>';
            }
        }
        
        // Mettre à jour le compteur
        const queueSize = document.getElementById('queue-size');
        if (queueSize) {
            queueSize.textContent = data.queue_size || 0;
        }
        
    } catch (error) {
        console.error('Erreur récupération tâches:', error);
    }
}

function createTaskCard(task, type) {
    const card = document.createElement('div');
    card.className = 'task-card';
    
    // Valeurs par défaut si undefined
    const taskId = task.id || 'N/A';
    const taskProduct = task.product || 'N/A';
    const taskDuration = task.duration || 0;
    const taskPriority = task.priority || 'NORMALE';
    const priorityClass = taskPriority.toLowerCase();
    
    if (type === 'queue') {
        card.innerHTML = `
            <div class="task-header">
                <span class="task-id">${taskId}</span>
                <span class="priority ${priorityClass}">${taskPriority}</span>
            </div>
            <div class="task-info">
                <div class="info-item">
                    <span class="label">📦 Produit:</span>
                    <span class="value">${taskProduct}</span>
                </div>
                <div class="info-item">
                    <span class="label">⏱️ Durée:</span>
                    <span class="value">${taskDuration} min</span>
                </div>
            </div>
        `;
    } else {
        // Task in progress
        const taskMachine = task.machine || 'N/A';
        const progress = task.progress || 0;
        const timeRemaining = task.time_remaining || 0;
        
        card.innerHTML = `
            <div class="task-header">
                <span class="task-id">${taskId}</span>
                <span class="machine-badge">${taskMachine}</span>
            </div>
            <div class="task-info">
                <div class="info-item">
                    <span class="label">📦 Produit:</span>
                    <span class="value">${taskProduct}</span>
                </div>
                <div class="info-item">
                    <span class="label">⏱️ Temps restant:</span>
                    <span class="value">${timeRemaining} min</span>
                </div>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${(progress * 100).toFixed(0)}%"></div>
            </div>
            <div class="progress-text">${(progress * 100).toFixed(0)}%</div>
        `;
    }
    
    return card;
}

// ==================== CONTRÔLE DES SIMULATEURS ====================

async function updateSimulatorsStatus() {
    try {
        const response = await fetch(`${API_BASE}/simulators/status`);
        const data = await response.json();
        
        // Statut capteurs
        const capteursStatus = document.getElementById('capteurs-status');
        if (capteursStatus) {
            if (data.capteurs.running) {
                capteursStatus.textContent = '🟢 En marche';
                capteursStatus.className = 'simulator-status running';
            } else {
                capteursStatus.textContent = '⚪ Arrêté';
                capteursStatus.className = 'simulator-status stopped';
            }
        }
        
        // Statut production
        const productionStatus = document.getElementById('production-status');
        if (productionStatus) {
            if (data.production.running) {
                productionStatus.textContent = '🟢 En marche';
                productionStatus.className = 'simulator-status running';
            } else {
                productionStatus.textContent = '⚪ Arrêté';
                productionStatus.className = 'simulator-status stopped';
            }
        }
        
        // Mettre à jour les sliders
        const capteursFreq = document.getElementById('capteurs-frequency');
        if (capteursFreq) {
            capteursFreq.value = data.capteurs.frequency;
            const freqValue = document.getElementById('capteurs-frequency-value');
            if (freqValue) freqValue.textContent = `${data.capteurs.frequency}s`;
        }
        
        const capteursError = document.getElementById('capteurs-error-rate');
        if (capteursError) {
            const errorRate = Math.round(data.capteurs.error_rate * 100);
            capteursError.value = errorRate;
            const errorValue = document.getElementById('capteurs-error-rate-value');
            if (errorValue) errorValue.textContent = `${errorRate}%`;
        }
        
        const prodFreq = document.getElementById('production-frequency');
        if (prodFreq) {
            prodFreq.value = data.production.frequency;
            const freqValue = document.getElementById('production-frequency-value');
            if (freqValue) freqValue.textContent = `${data.production.frequency}s`;
        }
        
    } catch (error) {
        console.error('Erreur récupération statut simulateurs:', error);
    }
}

async function startCapteursSimulator() {
    try {
        const response = await fetch(`${API_BASE}/simulators/capteurs/start`, { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            alert('✅ ' + data.message);
            updateSimulatorsStatus();
        } else {
            alert('❌ Erreur: ' + (data.detail || 'Erreur inconnue'));
        }
    } catch (error) {
        alert('❌ Erreur lors du démarrage: ' + error);
    }
}

async function stopCapteursSimulator() {
    try {
        const response = await fetch(`${API_BASE}/simulators/capteurs/stop`, { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            alert('✅ ' + data.message);
            updateSimulatorsStatus();
        }
    } catch (error) {
        alert('❌ Erreur: ' + error);
    }
}

function updateCapteursFrenquency(value) {
    const element = document.getElementById('capteurs-frequency-value');
    if (element) element.textContent = `${value}s`;
    
    fetch(`${API_BASE}/simulators/capteurs/config`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({frequency: parseInt(value)})
    }).catch(err => console.error('Erreur:', err));
}

function updateCapteursErrorRate(value) {
    const element = document.getElementById('capteurs-error-rate-value');
    if (element) element.textContent = `${value}%`;
    
    fetch(`${API_BASE}/simulators/capteurs/config`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({error_rate: parseFloat(value) / 100})
    }).catch(err => console.error('Erreur:', err));
}

async function forceError() {
    const machineId = document.getElementById('force-error-machine').value;
    
    if (!confirm(`⚠️ Forcer une panne sur ${machineId} ?`)) return;
    
    try {
        const response = await fetch(`${API_BASE}/simulators/capteurs/force-error`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({machine_id: machineId})
        });
        const data = await response.json();
        
        if (data.success) alert('🔴 ' + data.message);
    } catch (error) {
        alert('❌ Erreur: ' + error);
    }
}

async function startProductionSimulator() {
    try {
        const response = await fetch(`${API_BASE}/simulators/production/start`, { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            alert('✅ ' + data.message);
            updateSimulatorsStatus();
        }
    } catch (error) {
        alert('❌ Erreur: ' + error);
    }
}

async function stopProductionSimulator() {
    try {
        const response = await fetch(`${API_BASE}/simulators/production/stop`, { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            alert('✅ ' + data.message);
            updateSimulatorsStatus();
        }
    } catch (error) {
        alert('❌ Erreur: ' + error);
    }
}

function updateProductionFrequency(value) {
    const element = document.getElementById('production-frequency-value');
    if (element) element.textContent = `${value}s`;
    
    fetch(`${API_BASE}/simulators/production/config`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({frequency: parseInt(value)})
    }).catch(err => console.error('Erreur:', err));
}

// ==================== RAFRAÎCHISSEMENT AUTOMATIQUE ====================

function startAutoRefresh() {
    // Rafraîchir toutes les 5 secondes
    autoRefreshInterval = setInterval(() => {
        updateTime();
        fetchStats();
        fetchMachines();
        fetchTasks();
        updateSimulatorsStatus();
    }, 5000);
}

function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// ==================== INITIALISATION ====================

document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 Resource Manager Interface - Ahmed Belhouchette (@AhmedBelhouchette10)');
    
    // Premier chargement
    updateTime();
    fetchStats();
    fetchMachines();
    fetchTasks();
    updateSimulatorsStatus();
    
    // Démarrer le rafraîchissement automatique
    startAutoRefresh();
});