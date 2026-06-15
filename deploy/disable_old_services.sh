#!/bin/bash
# NurtacCoreEngineClaude: Eski 19 servisi disable et, supervisor'u enable et

echo "[DEPLOY] Eski servisleri disable ediliyor..."

# Tüm eski servisleri disable et
systemctl disable \
    nurtac-detector \
    nurtac-gate \
    nurtac-smartmoney \
    nurtac-evidence \
    nurtac-context \
    nurtac-volprofile \
    nurtac-scenario \
    nurtac-observer \
    nurtac-outcome \
    nurtac-paper \
    nurtac-reporter \
    nurtac-edge \
    nurtac-final

echo "[DEPLOY] Supervisor'u enable ediliyor..."
systemctl enable nurtac-supervisor

echo "[DEPLOY] Eski servisleri durduruluyor..."
systemctl stop \
    nurtac-detector \
    nurtac-gate \
    nurtac-smartmoney \
    nurtac-evidence \
    nurtac-context \
    nurtac-volprofile \
    nurtac-scenario \
    nurtac-observer \
    nurtac-outcome \
    nurtac-paper \
    nurtac-reporter \
    nurtac-edge \
    nurtac-final

echo "[DEPLOY] Supervisor başlatılıyor..."
systemctl start nurtac-supervisor

echo "[DEPLOY] Deployment tamamlandı!"
echo "[DEPLOY] Durumu kontrol et: systemctl status nurtac-supervisor"
echo "[DEPLOY] Logları izle: journalctl -u nurtac-supervisor -f"
