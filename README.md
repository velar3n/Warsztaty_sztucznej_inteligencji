*Autor: Natalia Kiełbasa*

# Warsztaty sztucznej inteligencji I & II
# Projektowanie systemów uczenia maszynowego dla chemi lekowej (GNN i LLM)

## Opis zadania
Budowa systemu typu MVP, który przewiduje aktywność biologiczną związków chemicznych (IC50) na podstawie danych z ChemBL.

## Struktura projektu
- Zadanie: Regresja wartości pIC50
- Modele: Porównanie pprostego projektu MLP (Multi-Layer Perceptron) i zaawansowanego modelu grafowego (GNN)
- Agent LLM: Wykorzystanie lokalnego modelu językowego do zarządzania zapytaniami użytkownika, wywoływania narzędzie (RD-Kit) i interpretacji wyników
- Spark i Airflow

## Ocena projektu

## Ocena projektu

| Ocena | Model | LLM |
|---|---|---|
| **3.0** | Działający MLP + bazowy GNN | LLM odpowiada tekstem, brak wywoływania narzędzi |
| **3.5** | GNN z poprawną inżynierią cech (RD-Kit) + Scaffold Split | Wizualizacja 2D cząsteczek; LLM "widzi" wyniki modelu |
| **4.0** | Sieć typu GNN + BatchNorm/Dropout + logowanie w MLflow | Agent LLM samodzielnie wywołuje model GNN dla SMILES |
| **4.5** | Model z AUC >= 0.65 na Scaffold Split; obsługa błędnych SMILES | Wywołuje model i dodatkowe narzędzia RD-Kit |
| **5.0** | AUC >= 0.70 lub 0.65 przy głębokiej analizie błędów | Planuje kroki, wizualizuje i interpretuje wyniki w kontekście chemicznym |

## Deadline: 
13. czerwca 2026

## Uruchomienie
```
docker compose up --build -d
```

## Kontenery
    - "Spark Master: http://localhost:8080"
    - "Airflow: http://localhost:8081"
