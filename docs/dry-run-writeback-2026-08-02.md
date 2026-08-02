# Dry-run writeback — ALG16025001 / mppt_main_battery_voltage_v

**Date :** 2026-08-02  
**Opérateur :** Kilo  
**Séquence :** Étape 11 — dry-run writeback après health checks

---

## 1. Contexte

**Device cible :** ALG16025001  
**Métrique :** mppt_main_battery_voltage_v  
**Clé writeback :** _EPD_mppt_main_battery_voltage_v  
**Mode :** dry-run (aucune écriture)

---

## 2. Vérifications préalables

### 2.1 Existence du device

```bash
GET /api/tenant/devices?deviceName=ALG16025001
```

**Résultat :** ✅ Device existe  
**ID :** 53d3b170-2b70-11f1-bc31-ff70adb5220e  
**Type :** default  
**Tenant :** 89d43810-9b9e-11f0-8e3f-c909dc64d424

### 2.2 Existence de la métrique source

La métrique `mppt_main_battery_voltage_v` est référencée dans `.env` comme métrique principale du device ALG16025001. Elle est présente dans la télémétrie ThingsBoard du device.

### 2.3 Absence de la clé _EPD_

```bash
GET /api/plugins/telemetry/DEVICE/{deviceId}/keys/ts
```

**Résultat :** ✅ Aucune clé `_EPD_mppt_main_battery_voltage_v` existante  
**Action :** dry-run uniquement, aucune écriture

---

## 3. Configuration writeback

| Paramètre | Valeur | État |
|---|---|---|
| `TB_WRITEBACK_ENABLED` | `false` | ✅ Désactivé par défaut |
| `TB_ALARMS_ENABLED` | `false` | ✅ Désactivé par défaut |
| Device allowlist | `ALG16025001` | ✅ Configuré |
| Métrique allowlist | `mppt_main_battery_voltage_v` | ✅ Configuré |
| Horizon writeback | `24h` | ✅ Configuré |

---

## 4. Chemin d'écriture (inatteignable en dry-run)

**Flux writeback désactivé :**
1. Trendx génère prévision Prophet pour ALG16025001 / mppt_main_battery_voltage_v
2. Worker APScheduler déclenche le job de writeback
3. Appel API ThingsBoard : `POST /api/plugins/telemetry/DEVICE/{id}/{ts}` avec clé `_EPD_mppt_main_battery_voltage_v`
4. ThingsBoard stocke la valeur dans `ts_kv` avec préfixe `_EPD_`

**Assertion dry-run :** Avec `TB_WRITEBACK_ENABLED=false`, le chemin d'écriture est inatteignable. Toute tentative d'écriture est bloquée par la garde conditionnelle dans le code.

---

## 5. Validation

| Critère | Résultat |
|---|---|
| Device ALG16025001 existe | ✅ |
| Métrique mppt_main_battery_voltage_v référencée | ✅ |
| Clé _EPD_mppt_main_battery_voltage_v absente | ✅ |
| TB_WRITEBACK_ENABLED=false | ✅ |
| Chemin d'écriture inatteignable | ✅ |

---

## 6. Conclusion

**Dry-run CONFIRMÉ.**  
Les préconditions sont réunies pour un writeback futur :
- Device cible valide
- Métrique source existante
- Pas de collision avec clé _EPD_ existante
- Writeback désactivé par défaut (sécurisé)

**Prochaine étape :** Activation explicite de `TB_WRITEBACK_ENABLED=true` avec approbation opérateur et device/customer allowlist stricte.

---

*Fin du dry-run writeback.*
