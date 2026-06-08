*Autor: Natalia Kiełbasa*

# Warsztaty sztucznej inteligencji I & II
# Projektowanie systemów uczenia maszynowego dla chemi lekowej (GNN i LLM)

## Opis zadania
Budowa systemu typu MVP, który przewiduje aktywność biologiczną związków chemicznych (IC50) na podstawie danych z ChemBL.

## Struktura projektu
- Zadanie: Regresja wartości pIC50
- Modele: Porównanie pprostego projektu MLP (Multi-Layer Perceptron) i zaawansowanego modelu grafowego (GNN)
- Agent LLM: Wykorzystanie lokalnego modelu językowego do zarządzania zapytaniami użytkownika, wywoływania narzędzie (RD-Kit) i interpretacji wyników

## Ocena projektu

## Ocena projektu

| Ocena | Model                                                          | LLM |
|---|----------------------------------------------------------------|---|
| **3.0** |  + bazowy GNN                                                  | LLM odpowiada tekstem, brak wywoływania narzędzi |
| **3.5** | GNN z poprawną inżynierią cech (RD-Kit) + Scaffold Split       | Wizualizacja 2D cząsteczek; LLM "widzi" wyniki modelu |
| **4.0** | Sieć typu GNN + BatchNorm/Dropout + logowanie w MLflow         | Agent LLM samodzielnie wywołuje model GNN dla SMILES |
| **4.5** | Model z AUC >= 0.65 na Scaffold Split; obsługa błędnych SMILES | Wywołuje model i dodatkowe narzędzia RD-Kit |
| **5.0** | AUC >= 0.70 lub 0.65 przy głębokiej analizie błędów            | Planuje kroki, wizualizuje i interpretuje wyniki w kontekście chemicznym |

## Deadline: 
13. czerwca 2026

## Wyniki Baseline test na małej grupie pierwszysz 100k próbek
### Baza (3 warstwy, 0.0003 lr, 0.1 dropout, 100 max epok)


| Model | Split Type | Loss | R² (Val) | R² (Test) | MSE (Val) | Epochs Trained | Best Epoch |
|---|---|---|---:|---:|---:|---:|---:|
| MLP | Random | MSE | 0.6474 | 0.6558 | 0.8548 | 19 | 7 |
| MLP | Scaffold | MSE | 0.4785 | 0.3897 | 1.2691 | 32 | 20 |
| GNN | Random | MSE | 0.1694 | 0.4194 | 2.0138 | 100 | 99 |
| GNN | Scaffold | MSE | 0.3221 | 0.3050 | 1.6495 | 47 | 35 |


## LLM setting

1. Interfejs:
- chat? bardziej a la wyszukiwarka? czy coś podobneo? Użytkownik musi być w stanie zadać pytanie i otrzxymać jakąś najs odpowiedź, ale czy zakładamy tez dopytywanie czy formę 1 pytanie --> 1 odpowiedź i brak potrzeby zapamiętywania historii?

Architektura:
Zapytanie jako SMILES -> ekstrakcja SMILES z zapytania jeśli pytanie w formie tekstowej (LLM) -> agent wywołuje model, czyli przekazanie SMILES do modelu -> wyciąga cechy z wyniku -> ładna odpowieź z potrzebnymi cechami -> zwracamy na front -> tworzy wizualizacje

Model:
- popatrzeć na huggin face
- ogólnie open source
- może coś trenowanego na danych typu tych co są w chembl (chemiczne/biomolekularne)
