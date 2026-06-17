"""
Logique de sélection des machines pour l'assignation des tâches
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


class MachineSelector:
    """Sélectionne la meilleure machine pour une tâche donnée"""
    
    # Constantes de configuration (ajustables)
    SEUIL_PROBABILITE_PANNE = 0.7
    SEUIL_TEMPERATURE_MAX = 90.0
    SEUIL_CHARGE_MAX = 80.0
    TAUX_PRODUCTION = 100  # unités par minute (quantite / TAUX = durée en minutes)
    
    @staticmethod
    def calculer_duree_tache(quantite: int) -> int:
        """
        Calcule la durée estimée d'une tâche en minutes
        
        Args:
            quantite: Nombre d'unités à produire
            
        Returns:
            Durée en minutes
        """
        return max(1, int(quantite / MachineSelector.TAUX_PRODUCTION))
    
    @staticmethod
    def machine_est_disponible(
        machine_id: str,
        etat_machine: Dict,
        alerte_ml: Optional[Dict],
        machines_occupees: Dict[str, datetime]
    ) -> Tuple[bool, str]:
        """
        Vérifie si une machine est disponible pour recevoir une tâche
        
        Args:
            machine_id: ID de la machine
            etat_machine: État actuel de la machine (depuis InfluxDB)
            alerte_ml: Alerte ML pour cette machine (si existe)
            machines_occupees: Dict des machines occupées {machine_id: fin_estimee}
            
        Returns:
            (disponible: bool, raison: str)
        """
        maintenant = datetime.utcnow()
        
        # Vérifier si la machine est occupée par une tâche
        if machine_id in machines_occupees:
            fin_estimee = machines_occupees[machine_id]
            if fin_estimee > maintenant:
                temps_restant = (fin_estimee - maintenant).total_seconds() / 60
                return False, f"Occupée (encore {int(temps_restant)} min)"
        
        # Vérifier l'état de la machine
        etat = etat_machine.get('etat', 'inconnu')
        if etat == 'arrete':
            return False, "Machine arrêtée"
        
        # Vérifier les alertes ML
        if alerte_ml:
            action = alerte_ml.get('action_recommandee', 'CONTINUER')
            probabilite = alerte_ml.get('probabilite_panne', 0)
            
            if action == 'ARRETER':
                return False, "ML recommande ARRÊT"
            
            if probabilite > MachineSelector.SEUIL_PROBABILITE_PANNE:
                return False, f"Probabilité panne élevée ({probabilite:.0%})"
        
        # Vérifier les métriques
        temperature = etat_machine.get('temperature', 0)
        if temperature > MachineSelector.SEUIL_TEMPERATURE_MAX:
            return False, f"Température trop élevée ({temperature}°C)"
        
        charge = etat_machine.get('charge_travail', 0)
        if charge > MachineSelector.SEUIL_CHARGE_MAX:
            return False, f"Charge trop élevée ({charge}%)"
        
        return True, "Disponible"
    
    @staticmethod
    def selectionner_meilleure_machine(
        machines_ids: List[str],
        etats_machines: Dict[str, Dict],
        alertes_ml: Dict[str, Dict],
        machines_occupees: Dict[str, datetime]
    ) -> Optional[Tuple[str, str]]:
        """
        Sélectionne la meilleure machine parmi toutes les machines
        
        Args:
            machines_ids: Liste des IDs de machines
            etats_machines: États actuels des machines
            alertes_ml: Alertes ML par machine
            machines_occupees: Machines occupées avec leur fin estimée
            
        Returns:
            (machine_id, raison) ou None si aucune machine disponible
        """
        machines_disponibles = []
        raisons_indisponibilite = {}
        
        # Filtrer les machines disponibles
        for machine_id in machines_ids:
            etat = etats_machines.get(machine_id, {})
            alerte = alertes_ml.get(machine_id)
            
            disponible, raison = MachineSelector.machine_est_disponible(
                machine_id, etat, alerte, machines_occupees
            )
            
            if disponible:
                machines_disponibles.append({
                    'machine_id': machine_id,
                    'etat': etat,
                    'alerte': alerte
                })
            else:
                raisons_indisponibilite[machine_id] = raison
        
        # Si aucune machine disponible
        if not machines_disponibles:
            raisons = ", ".join([f"{mid}: {r}" for mid, r in raisons_indisponibilite.items()])
            return None, f"Aucune machine disponible ({raisons})"
        
        # Score chaque machine (plus le score est bas, mieux c'est)
        scores = []
        
        for machine in machines_disponibles:
            machine_id = machine['machine_id']
            etat = machine['etat']
            alerte = machine['alerte']
            
            score = 0
            
            # Pénalité selon la charge actuelle (priorité principale)
            charge = etat.get('charge_travail', 0)
            score += charge * 2
            
            # Pénalité selon la température
            temperature = etat.get('temperature', 70)
            if temperature > 80:
                score += (temperature - 80) * 1.5
            
            # Bonus si ML recommande CONTINUER
            if alerte and alerte.get('action_recommandee') == 'CONTINUER':
                score -= 15
            
            # Pénalité si ML recommande PAUSE
            if alerte and alerte.get('action_recommandee') == 'PAUSE':
                score += 25
            
            # Légère pénalité selon la probabilité de panne
            if alerte:
                prob = alerte.get('probabilite_panne', 0)
                score += prob * 10
            
            scores.append((machine_id, score))
        
        # Trier par score (le plus bas en premier)
        scores.sort(key=lambda x: x[1])
        
        meilleure_machine = scores[0][0]
        return meilleure_machine, f"Sélectionnée (score: {scores[0][1]:.1f})"