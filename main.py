"""
Docstring for main

1. Join tables:
    - activity as main table
        - add assays on assay_id (wszystko assay_ ..., )
        - add docs on doc_id
        - molecule_dictionary on molregno
            - add compound_structures on molregno (smilesy)

2. Remove useless columns

3. Remove rows with:
    - target organism not homo sapiens
    - standard value + standard units nie null
    - na razie standard type nie IC50

    Potencjalnie
    - assay type --> B (Binding (B) - Data measuring binding of compound to a molecular target, e.g. Ki, IC50, Kd.)
    - potencjalnie pchembl value zamiast standard value + jednostka
    - zwrócić uwagę na istnienie ACTIVITY_STDS_LOOKUP --> wskazujące na outliery
    - na razie wybrac 

4. Ujednolicenie jednostek standard_value + jednostka --> jedne coś

5. Zmienić opisowe dane na dane liczbowe

6. Przeliczyć ilość og rzędów --> nowa ilość rzędów

7. Gdzieś zapisać nową tabelę to treningu jako V_X w bazie danych

"""
